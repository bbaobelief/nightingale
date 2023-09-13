package memsto

import (
	"context"
	"fmt"
	"github.com/didi/nightingale/v5/src/pkg/cmdb"
	"github.com/didi/nightingale/v5/src/server/config"
	promstat "github.com/didi/nightingale/v5/src/server/stat"
	"github.com/didi/nightingale/v5/src/storage"
	"github.com/toolkits/pkg/logger"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"strconv"
	"sync"
	"time"
)

type IdentClusterCacheType struct {
	statTotal       int64
	statLastUpdated int64

	sync.RWMutex
	idents map[string]string
}

var IdentClusterCache = IdentClusterCacheType{
	statTotal:       -1,
	statLastUpdated: -1,
	idents:          make(map[string]string),
}

func (ic *IdentClusterCacheType) Reset() {
	ic.Lock()
	defer ic.Unlock()

	ic.statTotal = -1
	ic.statLastUpdated = -1
	ic.idents = make(map[string]string)
}

func (ic *IdentClusterCacheType) StatChanged(total, lastUpdated int64) bool {
	if ic.statTotal == total && ic.statLastUpdated == lastUpdated {
		return false
	}

	return true
}

func (ic *IdentClusterCacheType) Set(idents map[string]string, total, lastUpdated int64) {
	ic.Lock()
	ic.idents = idents
	ic.Unlock()

	// only one goroutine used, so no need lock
	ic.statTotal = total
	ic.statLastUpdated = lastUpdated
}

func (ic *IdentClusterCacheType) Get(ident string) (string, bool) {
	ic.RLock()
	defer ic.RUnlock()
	cluster, has := ic.idents[ident]
	return cluster, has
}

func (ic *IdentClusterCacheType) Put(ident, cluster string) {
	ic.Lock()
	ic.idents[ident] = cluster
	ic.Unlock()
}

func redisKey(ident string) string {
	return fmt.Sprintf("idents:%s", ident)
}

func IdentToCluster(ident string) string {
	if ident == "" {
		return config.C.IdentOpt.DefaultCluster
	}

	caser := cases.Title(language.English)
	if cluster, ok := IdentClusterCache.Get(redisKey(ident)); ok {
		return caser.String(cluster)
	}

	cluster := cmdb.IdentByCmdb(ident)
	cluster = caser.String(cluster)

	if cluster != "" {
		expire := time.Duration(config.C.IdentOpt.CacheExpiration) * time.Millisecond
		err := storage.Redis.Set(context.Background(), redisKey(ident), cluster, expire).Err()
		if err != nil {
			logger.Errorf("failed to Set ident: %s cluster: %s, err: %v", ident, cluster, err)
		}
		err = storage.Redis.Set(context.Background(), redisKey("lastUpdate"), time.Now().Unix(), expire).Err()
		if err != nil {
			logger.Errorf("failed to Set lastUpdate ident: %s time: %s, err: %v", ident, time.Now().Unix(), err)
		}
	} else {
		cluster = config.C.IdentOpt.DefaultCluster
		IdentClusterCache.Put(redisKey(ident), cluster)
	}
	logger.Infof("IdentToCluster ident: %s cluster: %s", ident, cluster)
	return cluster
}

func SyncIdentCluster() {
	err := syncIdentCluster()
	if err != nil {
		fmt.Println("failed to sync ident cluster:", err)
		exit(1)
	}
	go loopSyncIdentCluster()
}

func loopSyncIdentCluster() {
	duration := time.Duration(config.C.IdentOpt.CacheInterval) * time.Millisecond
	for {
		time.Sleep(duration)
		if err := syncIdentCluster(); err != nil {
			logger.Warning("failed to sync ident cluster:", err)
		}
	}
}

func syncIdentCluster() error {
	start := time.Now()
	ctx := context.Background()

	idents := identGetAll(ctx)
	lastStr, ok := idents[redisKey("lastUpdate")]
	if ok {
		delete(idents, redisKey("lastUpdate"))
	}

	lastUpdate, err := strconv.ParseInt(lastStr, 10, 64)
	if err != nil {
		logger.Error("failed to get ident lastUpdate, err:", err)
	}

	total := len(idents)
	if !IdentClusterCache.StatChanged(int64(total), lastUpdate) {
		promstat.GaugeCronDuration.WithLabelValues("sync_ident_cluster").Set(0)
		promstat.GaugeSyncNumber.WithLabelValues("sync_ident_cluster").Set(0)
		logger.Debug("ident cluster not changed")
		return nil
	}

	if total > 0 {
		IdentClusterCache.Set(idents, int64(total), lastUpdate)
	}

	ms := time.Since(start).Milliseconds()
	promstat.GaugeCronDuration.WithLabelValues("sync_ident_cluster").Set(float64(ms))
	promstat.GaugeSyncNumber.WithLabelValues("sync_ident_cluster").Set(float64(total))
	logger.Infof("timer: sync ident_cluster done, cost: %dms, number: %d", ms, total)
	return nil
}

func identGetAll(ctx context.Context) (idents map[string]string) {
	idents = make(map[string]string)
	keys, cursor, err := scanIdentKeys(ctx, "idents:*", 100)
	if err != nil {
		logger.Errorf("failed to exec redis Scan identKeys, cursor:%d err: %v", cursor, err)
		return
	}

	if len(keys) == 0 {
		return idents
	}

	values, err := storage.Redis.MGet(ctx, keys...).Result()
	if err != nil {
		logger.Errorf("failed to exec redis MGet, err: %v", err)
		return
	}

	for i, key := range keys {
		value := values[i]
		val, ok := value.(string)
		if !ok {
			logger.Warningf("failed to exec redis MGet, key: %s value: %v", key, value)
			continue
		}
		idents[key] = val
	}
	return idents
}

func scanIdentKeys(ctx context.Context, matchPattern string, count int64) (keys []string, cursor uint64, err error) {
	for {
		scanKeys, cursor, err := storage.Redis.Scan(ctx, cursor, matchPattern, count).Result()
		if err != nil {
			return nil, 0, err
		}
		keys = append(keys, scanKeys...)
		if cursor == 0 {
			break
		}
	}
	return keys, cursor, nil
}
