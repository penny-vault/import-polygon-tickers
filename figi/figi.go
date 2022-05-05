package figi

import (
	"github.com/penny-vault/import-tickers/common"
	"github.com/spf13/viper"
)

func LookupFigi(asset *common.Asset) string {
	apiKey := viper.GetString("openfigi_apikey")
	return ""
}
