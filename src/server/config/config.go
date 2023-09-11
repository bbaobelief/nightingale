package config

import (
	"fmt"
	"html/template"
	"log"
	"os"
	"path"
	"plugin"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/koding/multiconfig"
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/file"
	"github.com/toolkits/pkg/runner"

	"github.com/didi/nightingale/v5/src/models"
	"github.com/didi/nightingale/v5/src/notifier"
	"github.com/didi/nightingale/v5/src/pkg/httpx"
	"github.com/didi/nightingale/v5/src/pkg/logx"
	"github.com/didi/nightingale/v5/src/pkg/ormx"
	"github.com/didi/nightingale/v5/src/pkg/secu"
	"github.com/didi/nightingale/v5/src/pkg/tplx"
	"github.com/didi/nightingale/v5/src/storage"
)

var (
	C    = new(Config)
	once sync.Once
)

func DealConfigCrypto(key string) {
	decryptDsn, err := secu.DealWithDecrypt(C.DB.DSN, key)
	if err != nil {
		fmt.Println("failed to decrypt the db dsn", err)
		os.Exit(1)
	}
	C.DB.DSN = decryptDsn

	decryptRedisPwd, err := secu.DealWithDecrypt(C.Redis.Password, key)
	if err != nil {
		fmt.Println("failed to decrypt the redis password", err)
		os.Exit(1)
	}
	C.Redis.Password = decryptRedisPwd

	decryptSmtpPwd, err := secu.DealWithDecrypt(C.SMTP.Pass, key)
	if err != nil {
		fmt.Println("failed to decrypt the smtp password", err)
		os.Exit(1)
	}
	C.SMTP.Pass = decryptSmtpPwd

	decryptHookPwd, err := secu.DealWithDecrypt(C.Alerting.Webhook.BasicAuthPass, key)
	if err != nil {
		fmt.Println("failed to decrypt the alert webhook password", err)
		os.Exit(1)
	}
	C.Alerting.Webhook.BasicAuthPass = decryptHookPwd

	decryptIbexPwd, err := secu.DealWithDecrypt(C.Ibex.BasicAuthPass, key)
	if err != nil {
		fmt.Println("failed to decrypt the ibex password", err)
		os.Exit(1)
	}
	C.Ibex.BasicAuthPass = decryptIbexPwd

	for _, cluster := range C.Clusters {
		decryptReaderPwd, err := secu.DealWithDecrypt(cluster.Reader.BasicAuthPass, key)
		if err != nil {
			fmt.Printf("failed to decrypt the reader password: %s , error: %s", cluster.Reader.BasicAuthPass, err.Error())
			os.Exit(1)
		}
		cluster.Reader.BasicAuthPass = decryptReaderPwd

		for index, v := range cluster.Writers {
			decryptWriterPwd, err := secu.DealWithDecrypt(v.BasicAuthPass, key)
			if err != nil {
				fmt.Printf("failed to decrypt the writer password: %s , error: %s", v.BasicAuthPass, err.Error())
				os.Exit(1)
			}
			cluster.Writers[index].BasicAuthPass = decryptWriterPwd
		}
	}
}

func MustLoad(key string, fpaths ...string) {
	once.Do(func() {
		loaders := []multiconfig.Loader{
			&multiconfig.TagLoader{},
			&multiconfig.EnvironmentLoader{},
		}

		for _, fpath := range fpaths {
			handled := false

			if strings.HasSuffix(fpath, "toml") {
				loaders = append(loaders, &multiconfig.TOMLLoader{Path: fpath})
				handled = true
			}
			if strings.HasSuffix(fpath, "conf") {
				loaders = append(loaders, &multiconfig.TOMLLoader{Path: fpath})
				handled = true
			}
			if strings.HasSuffix(fpath, "json") {
				loaders = append(loaders, &multiconfig.JSONLoader{Path: fpath})
				handled = true
			}
			if strings.HasSuffix(fpath, "yaml") {
				loaders = append(loaders, &multiconfig.YAMLLoader{Path: fpath})
				handled = true
			}

			if !handled {
				fmt.Println("config file invalid, valid file exts: .conf,.yaml,.toml,.json")
				os.Exit(1)
			}
		}

		m := multiconfig.DefaultLoader{
			Loader:    multiconfig.MultiLoader(loaders...),
			Validator: multiconfig.MultiValidator(&multiconfig.RequiredValidator{}),
		}
		m.MustLoad(C)

		DealConfigCrypto(key)

		if C.EngineDelay == 0 {
			C.EngineDelay = 120
		}

		if C.ReaderFrom == "" {
			C.ReaderFrom = "config"
		}

		if C.ReaderFrom == "config" && len(C.Clusters) == 0 {
			fmt.Println("configuration Cluster is blank")
			os.Exit(1)
		}

		if C.Heartbeat.IP == "" {
			hostname, err := os.Hostname()
			if err != nil {
				fmt.Println("failed to get hostname:", err)
				os.Exit(1)
			}

			if strings.Contains(hostname, "localhost") {
				fmt.Println("Warning! hostname contains substring localhost, setting a more unique hostname is recommended")
			}

			C.Heartbeat.IP = hostname
		}

		C.Heartbeat.Endpoint = fmt.Sprintf("%s:%d", C.Heartbeat.IP, C.HTTP.Port)

		C.Alerting.check()

		if C.WriterOpt.QueueMaxSize <= 0 {
			C.WriterOpt.QueueMaxSize = 10000000
		}

		if C.WriterOpt.QueuePopSize <= 0 {
			C.WriterOpt.QueuePopSize = 1000
		}

		if C.WriterOpt.QueueCount <= 0 {
			C.WriterOpt.QueueCount = 1000
		}

		if C.WriterOpt.ShardingKey == "" {
			C.WriterOpt.ShardingKey = "ident"
		}

		for _, cluster := range C.Clusters {
			for _, write := range cluster.Writers {
				for _, relabel := range write.WriteRelabels {
					regex, ok := relabel.Regex.(string)
					if !ok {
						log.Println("Regex field must be a string")
						os.Exit(1)
					}

					if regex == "" {
						regex = "(.*)"
					}
					relabel.Regex = models.MustNewRegexp(regex)

					if relabel.Separator == "" {
						relabel.Separator = ";"
					}

					if relabel.Action == "" {
						relabel.Action = "replace"
					}

					if relabel.Replacement == "" {
						relabel.Replacement = "$1"
					}
				}
			}
		}

		fmt.Println("heartbeat.ip:", C.Heartbeat.IP)
		fmt.Printf("heartbeat.interval: %dms\n", C.Heartbeat.Interval)
	})
}

type Config struct {
	RunMode            string
	BusiGroupLabelKey  string
	EngineDelay        int64
	DisableUsageReport bool
	ReaderFrom         string
	LabelRewrite       bool
	ForceUseServerTS   bool
	Log                logx.Config
	HTTP               httpx.Config
	BasicAuth          gin.Accounts
	SMTP               SMTPConfig
	Heartbeat          HeartbeatConfig
	Alerting           Alerting
	NoData             NoData
	Redis              storage.RedisConfig
	DB                 ormx.DBConfig
	WriterOpt          WriterGlobalOpt
	Clusters           []Clusters
	Ibex               Ibex
}

type Clusters struct {
	Name    string
	Reader  PromOption
	Writers []WriterOptions
}

type WriterOptions struct {
	Url           string
	BasicAuthUser string
	BasicAuthPass string

	Timeout               int64
	DialTimeout           int64
	TLSHandshakeTimeout   int64
	ExpectContinueTimeout int64
	IdleConnTimeout       int64
	KeepAlive             int64

	MaxConnsPerHost     int
	MaxIdleConns        int
	MaxIdleConnsPerHost int

	Headers []string

	WriteRelabels []*models.RelabelConfig
}

type WriterGlobalOpt struct {
	QueueCount   int
	QueueMaxSize int
	QueuePopSize int
	ShardingKey  string
}

type HeartbeatConfig struct {
	IP       string
	Interval int64
	Endpoint string
}

type SMTPConfig struct {
	Host               string
	Port               int
	User               string
	Pass               string
	From               string
	InsecureSkipVerify bool
	Batch              int
}

type Alerting struct {
	Timeout               int64
	TemplatesDir          string
	NotifyConcurrency     int
	NotifyBuiltinChannels []string
	CallScript            CallScript
	CallPlugin            CallPlugin
	RedisPub              RedisPub
	Webhook               Webhook
}

func (a *Alerting) check() {
	if a.Webhook.Enable {
		if a.Webhook.Timeout == "" {
			a.Webhook.TimeoutDuration = time.Second * 5
		} else {
			dur, err := time.ParseDuration(C.Alerting.Webhook.Timeout)
			if err != nil {
				fmt.Println("failed to parse Alerting.Webhook.Timeout")
				os.Exit(1)
			}
			a.Webhook.TimeoutDuration = dur
		}
	}

	if a.CallPlugin.Enable {
		if runtime.GOOS == "windows" {
			fmt.Println("notify plugin on unsupported os:", runtime.GOOS)
			os.Exit(1)
		}

		p, err := plugin.Open(a.CallPlugin.PluginPath)
		if err != nil {
			fmt.Println("failed to load plugin:", err)
			os.Exit(1)
		}

		caller, err := p.Lookup(a.CallPlugin.Caller)
		if err != nil {
			fmt.Println("failed to lookup plugin Caller:", err)
			os.Exit(1)
		}

		ins, ok := caller.(notifier.Notifier)
		if !ok {
			log.Println("notifier interface not implemented")
			os.Exit(1)
		}

		notifier.Instance = ins
	}

	if a.TemplatesDir == "" {
		a.TemplatesDir = path.Join(runner.Cwd, "etc", "template")
	}

	if a.Timeout == 0 {
		a.Timeout = 30000
	}
}

func (a *Alerting) ListTpls() (map[string]*template.Template, error) {
	filenames, err := file.FilesUnder(a.TemplatesDir)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to exec FilesUnder")
	}

	if len(filenames) == 0 {
		return nil, errors.New("no tpl files under " + a.TemplatesDir)
	}

	tplFiles := make([]string, 0, len(filenames))
	for i := 0; i < len(filenames); i++ {
		if strings.HasSuffix(filenames[i], ".tpl") {
			tplFiles = append(tplFiles, filenames[i])
		}
	}

	if len(tplFiles) == 0 {
		return nil, errors.New("no tpl files under " + a.TemplatesDir)
	}

	tpls := make(map[string]*template.Template)
	for _, tplFile := range tplFiles {
		tplpath := path.Join(a.TemplatesDir, tplFile)
		tpl, err := template.New(tplFile).Funcs(tplx.TemplateFuncMap).ParseFiles(tplpath)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to parse tpl: "+tplpath)
		}
		tpls[tplFile] = tpl
	}
	return tpls, nil
}

type CallScript struct {
	Enable     bool
	ScriptPath string
}

type CallPlugin struct {
	Enable     bool
	PluginPath string
	Caller     string
}

type RedisPub struct {
	Enable        bool
	ChannelPrefix string
	ChannelKey    string
}

type Webhook struct {
	Enable          bool
	Url             string
	BasicAuthUser   string
	BasicAuthPass   string
	Timeout         string
	TimeoutDuration time.Duration
	Headers         []string
}

type NoData struct {
	Metric   string
	Interval int64
}

type Ibex struct {
	Address       string
	BasicAuthUser string
	BasicAuthPass string
	Timeout       int64
}

func (c *Config) IsDebugMode() bool {
	return c.RunMode == "debug"
}
