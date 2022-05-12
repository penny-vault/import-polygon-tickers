/*
Copyright 2022

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package yfinance

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/penny-vault/import-tickers/common"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

var kUrls []string = []string{
	"https://query1.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=assetProfile%%2CfundProfile%%2Cprice%%2CesgScores&ssl=true",
	"https://query2.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=assetProfile%%2CfundProfile%%2Cprice%%2CesgScores&ssl=true",
}

// NOTE: These are sparse structs, only exctracting the information we need

type YFinanceResult struct {
	Profile *YFinanceQuoteSummaryWrapper `json:"quoteSummary"`
}

type YFinanceQuoteSummaryWrapper struct {
	Result []*YFinanceQuoteSummary `json:"result"`
}

type YFinanceQuoteSummary struct {
	AssetProfile *YFinanceAssetProfile `json:"assetProfile"`
	Price        *YFinancePrice        `json:"price"`
	Esg          *YFinanceESG          `json:"esgScores"`
}

type YFinanceAssetProfile struct {
	Website           string `json:"website"`
	Industry          string `json:"industry"`
	Sector            string `json:"sector"`
	Summary           string `json:"longBusinessSummary"`
	FullTimeEmployees int    `json:"fullTimeEmployees"`
}

type YFinancePrice struct {
	Name string `json:"longName"`
}

type YFinanceESG struct {
	PeerGroup string `json:"peerGroup"`
}

func RateLimit() *rate.Limiter {
	dur := time.Duration(int64(time.Second) * 60 / viper.GetInt64("yahoo.rate_limit"))
	yahooRate := rate.Every(dur)
	return rate.NewLimiter(yahooRate, 2)
}

func NumAssetsNeedingUpdate(assets []*common.Asset) int {
	totalCount := 0
	for _, asset := range assets {
		if asset.DelistingDate == "" && asset.AssetType == common.CommonStock && (asset.Industry == "" || asset.Sector == "" || asset.Description == "") {
			totalCount += 1
		}
		if asset.DelistingDate == "" && asset.AssetType == common.MutualFund && asset.Name == "" {
			totalCount += 1
		}
		if asset.DelistingDate == "" && asset.AssetType == common.ETF && asset.Description == "" {
			totalCount += 1
		}
	}
	return totalCount
}

func Enrich(assets []*common.Asset, max int) {
	yahooRateLimiter := RateLimit()

	numNeedingUpdate := NumAssetsNeedingUpdate(assets)
	log.Info().Int("NeedsUpdate", numNeedingUpdate).Msg("num assets needing meta-data update from yahoo")
	if numNeedingUpdate > max && max != 0 {
		numNeedingUpdate = max
	}
	bar := progressbar.Default(int64(numNeedingUpdate))

	count := make(chan int, len(assets))
	callCount := 0

	for _, asset := range assets {
		if asset.DelistingDate == "" && asset.AssetType == common.CommonStock && (asset.Industry == "" || asset.Sector == "" || asset.Description == "") {
			bar.Add(1)
			yahooRateLimiter.Wait(context.Background())
			callCount += 1
			go func(myAsset *common.Asset) {
				Download(myAsset)
				count <- 1
			}(asset)
		}
		if asset.DelistingDate == "" && asset.AssetType == common.MutualFund && asset.Name == "" {
			bar.Add(1)
			yahooRateLimiter.Wait(context.Background())
			callCount += 1
			go func(myAsset *common.Asset) {
				Download(myAsset)
				count <- 1
			}(asset)
		}
		if asset.DelistingDate == "" && asset.AssetType == common.ETF && asset.Description == "" {
			bar.Add(1)
			yahooRateLimiter.Wait(context.Background())
			callCount += 1
			go func(myAsset *common.Asset) {
				Download(myAsset)
				count <- 1
			}(asset)
		}

		if max > 0 && callCount > max {
			break
		}
	}

	// wait for all items to finish
	for callCount > 0 {
		v := <-count
		callCount -= v
	}

}

// Download retrieves data for the list of assets from Yahoo! Finance
func Download(asset *common.Asset) {
	n := rand.Intn(len(kUrls))
	url := fmt.Sprintf(kUrls[n], asset.Ticker)

	subLog := log.With().Str("Url", url).Str("Source", "yfinance").Logger()

	client := resty.New()
	resp, err := client.R().Get(url)

	if err != nil {
		subLog.Error().Stack().Err(err).Msg("error when fetching yahoo asset profile")
		return
	}

	if resp.StatusCode() >= 400 {
		subLog.Error().Int("StatusCode", resp.StatusCode()).Msg("invalid status code received from server")
		return
	}

	body := resp.Body()
	if err != nil {
		subLog.Error().Stack().Err(err).Msg("could not read response body when fetching assets")
		return
	}

	wrapper := YFinanceResult{}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		subLog.Error().Stack().Err(err).Msg("could not unmarshal response body when fetching assets")
		return
	}

	res := wrapper.Profile.Result
	if asset.Ticker == "MSFT" {
		fmt.Printf("%+v\n", res)
	}
	if len(res) == 1 {
		assetProfile := res[0].AssetProfile
		if assetProfile != nil {
			if asset.Description != assetProfile.Summary {
				asset.Description = assetProfile.Summary
				asset.LastUpdated = time.Now().Unix()
			}
			if asset.Industry != assetProfile.Industry {
				asset.Industry = assetProfile.Industry
				asset.LastUpdated = time.Now().Unix()
			}
			if asset.Sector != assetProfile.Sector {
				asset.Sector = assetProfile.Sector
				asset.LastUpdated = time.Now().Unix()
			}
			if asset.CorporateUrl != assetProfile.Website {
				asset.CorporateUrl = assetProfile.Website
				asset.LastUpdated = time.Now().Unix()
			}
		}
		price := res[0].Price
		if price != nil {
			if asset.Name == "" {
				asset.Name = price.Name
				asset.LastUpdated = time.Now().Unix()
			}
		}
		esg := res[0].Esg
		if esg != nil {
			if asset.Description == "" {
				asset.Description = esg.PeerGroup
				asset.LastUpdated = time.Now().Unix()
			}
		}

		return
	}
}
