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

	"github.com/go-resty/resty/v2"
	"github.com/penny-vault/import-tickers/common"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

var kUrls []string = []string{
	"https://query1.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=assetProfile%%2CfundProfile&ssl=true",
	"https://query2.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=assetProfile%%2CfundProfile&ssl=true",
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
}

type YFinanceAssetProfile struct {
	Website           string `json:"website"`
	Industry          string `json:"industry"`
	Sector            string `json:"sector"`
	Summary           string `json:"longBusinessSummary"`
	FullTimeEmployees int    `json:"fullTimeEmployees"`
}

// Download retrieves data for the list of assets from Yahoo! Finance
func Download(asset *common.Asset, limit *rate.Limiter) *YFinanceAssetProfile {
	result := YFinanceAssetProfile{}
	limit.Wait(context.Background())
	n := rand.Intn(len(kUrls))
	url := fmt.Sprintf(kUrls[n], asset.Ticker)

	subLog := log.With().Str("Url", url).Str("Source", "yfinance").Logger()

	client := resty.New()
	resp, err := client.R().Get(url)

	if err != nil {
		subLog.Error().Stack().Err(err).Msg("error when fetching yahoo asset profile")
		return nil
	}

	if resp.StatusCode() >= 400 {
		subLog.Error().Int("StatusCode", resp.StatusCode()).Msg("invalid status code received from server")
		return nil
	}

	body := resp.Body()
	if err != nil {
		log.Error().Stack().Err(err).Msg("could not read response body when fetching assets")
		return nil
	}

	wrapper := YFinanceResult{}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		subLog.Error().Stack().Err(err).Msg("could not unmarshal response body when fetching assets")
		return nil
	}

	res := wrapper.Profile.Result
	if len(res) == 1 {
		assetProfile := res[0].AssetProfile
		asset.Description = assetProfile.Summary
		asset.Industry = assetProfile.Industry
		asset.Sector = assetProfile.Sector
		asset.CorporateUrl = assetProfile.Website
	}

	return &result
}
