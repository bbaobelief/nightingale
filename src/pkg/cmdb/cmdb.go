package cmdb

import (
	"github.com/didi/nightingale/v5/src/server/config"
	"github.com/go-resty/resty/v2"
	"github.com/toolkits/pkg/logger"
	"time"
)

var client = resty.New()

func init() {
	client.SetTimeout(time.Duration(config.C.IdentOpt.Timeout))
}

type CmdbResponse struct {
	Total   int `json:"total"`
	Page    int `json:"page"`
	Pages   int `json:"pages"`
	Results []struct {
		Name        string `json:"name"`
		PrivateIp   string `json:"private_ip"`
		CompanyAbbr string `json:"company_abbr"`
	} `json:"results"`
}

func IdentByCmdb(ident string) string {
	result := &CmdbResponse{}
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"name":   ident,
			"fields": "company_abbr,name,private_ip",
		}).
		SetHeader(config.C.IdentOpt.CmdbKeyName, config.C.IdentOpt.CmdbKeyValue).
		SetResult(result).
		ForceContentType("application/json").
		Get(config.C.IdentOpt.CmdbUrl)

	if err != nil {
		logger.Printf("failed to get cmdb: status=%d err=%v", resp.StatusCode(), err)
		return ""
	}

	for _, v := range result.Results {
		cluster := v.CompanyAbbr
		return cluster
	}
	return ""
}
