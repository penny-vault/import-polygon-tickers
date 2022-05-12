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
package polygon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/penny-vault/import-tickers/common"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

type PolygonAssetsResponse struct {
	Results   []*PolygonAsset `json:"results"`
	Status    string          `json:"status"`
	RequestId string          `json:"request_id"`
	Count     int             `json:"count"`
	NextUrl   string          `json:"next_url"`
}

type PolygonAsset struct {
	Active          bool   `json:"active"`
	CIK             string `json:"cik"`
	CompositeFigi   string `json:"composite_figi"`
	CurrencyName    string `json:"currency_name"`
	LastUpdatedUTC  string `json:"last_updated_utc"`
	Locale          string `json:"locale"`
	Market          string `json:"market"`
	Name            string `json:"name"`
	PrimaryExchange string `json:"primary_exchange"`
	ShareClassFigi  string `json:"share_class_figi"`
	Ticker          string `json:"ticker"`
	Type            string `json:"type"`
}

type PolygonAssetDetailResponse struct {
	Result    *PolygonAssetDetail `json:"results"`
	Status    string              `json:"status"`
	RequestId string              `json:"request_id"`
}

type PolygonAddress struct {
	Address1   string `json:"address1"`
	City       string `json:"city"`
	State      string `json:"state"`
	PostalCode string `json:"postal_code"`
}

type PolygonBranding struct {
	LogoUrl string `json:"logo_url"`
	IconUrl string `json:"icon_url"`
}

type PolygonAssetDetail struct {
	Ticker                      string          `json:"ticker"`
	Name                        string          `json:"name"`
	Market                      string          `json:"market"`
	Locale                      string          `json:"locale"`
	PrimaryExchange             string          `json:"primary_exchange"`
	Type                        string          `json:"type"`
	Active                      bool            `json:"bool"`
	CurrencyName                string          `json:"currency_name"`
	CIK                         string          `json:"cik"`
	CompositeFigi               string          `json:"composite_figi"`
	ShareClassFigi              string          `json:"share_class_figi"`
	MarketCap                   float64         `json:"market_cap"`
	PhoneNumber                 string          `json:"phone_number"`
	Address                     *PolygonAddress `json:"address"`
	Description                 string          `json:"description"`
	SicCode                     string          `json:"sic_code"`
	SicDescription              string          `json:"sic_description"`
	TickerRoot                  string          `json:"ticker_root"`
	HomepageUrl                 string          `json:"homepage_url"`
	TotalEmployees              int             `json:"total_employees"`
	ListingDate                 string          `json:"list_date"`
	Branding                    PolygonBranding `json:"branding"`
	ShareClassSharesOutstanding int             `json:"share_class_shares_outstanding"`
	WeightedSharesOutstanding   int             `json:"weighted_shares_outstanding"`
}

func rateLimit() *rate.Limiter {
	dur := time.Duration(int64(time.Second) * 60 / viper.GetInt64("polygon.rate_limit"))
	polygonRate := rate.Every(dur)
	return rate.NewLimiter(polygonRate, 2)
}

func EnrichDetail(assets []*common.Asset, max int) {
	maxPolygonDetailAge := viper.GetInt64("polygon.detail_age")
	polygonRateLimiter := rateLimit()
	bar := progressbar.Default(int64(len(assets)))
	now := time.Now().Unix()
	count := 0
	for _, asset := range assets {
		bar.Add(1)
		count++
		if asset.AssetType != common.MutualFund && (asset.PolygonDetailAge+maxPolygonDetailAge) < now {
			FetchAssetDetail(asset, polygonRateLimiter)
			asset.PolygonDetailAge = now
		}
		if max > 0 && max < count {
			break
		}
	}

}

func FetchAssetDetail(asset *common.Asset, limit *rate.Limiter) *common.Asset {
	limit.Wait(context.Background())

	client := resty.New()

	urlClean := fmt.Sprintf("https://api.polygon.io/v3/reference/tickers/%s?apiKey=", asset.Ticker)
	url := fmt.Sprintf("%s%s", urlClean, viper.GetString("polygon.token"))
	subLog := log.With().Str("Url", urlClean).Str("Source", "polygon.io").Logger()

	resp, err := client.R().Get(url)

	if err != nil {
		subLog.Error().Err(err).Msg("error when fetching list of assets")
		return asset
	}

	if resp.StatusCode() >= 400 {
		subLog.Error().Int("StatusCode", resp.StatusCode()).Msg("error code received from server when fetching assets")
	}

	body := resp.Body()
	if err != nil {
		subLog.Error().Stack().Err(err).Msg("could not read response body when fetching assets")
		return asset
	}

	assetDetail := PolygonAssetDetailResponse{}
	if err := json.Unmarshal(body, &assetDetail); err != nil {
		subLog.Error().Stack().Err(err).Msg("could not unmarshal response body when fetching assets")
		return asset
	}

	if assetDetail.Status != "OK" {
		subLog.Error().Str("PolygonStatus", assetDetail.Status).Err(err).Msg("polygon status code not OK")
		return asset
	}

	asset.ListingDate = assetDetail.Result.ListingDate
	asset.CorporateUrl = assetDetail.Result.HomepageUrl
	asset.Description = assetDetail.Result.Description

	// fetch icon
	if assetDetail.Result.Branding.IconUrl != "" {
		asset.IconUrl = assetDetail.Result.Branding.IconUrl
	}

	return asset
}

func FetchIcon(url string, limit *rate.Limiter) []byte {
	limit.Wait(context.Background())
	subLog := log.With().Str("Url", url).Str("Source", "polygon.io").Logger()
	url = fmt.Sprintf("%s?apiKey=%s", url, viper.GetString("polygon.token"))

	client := resty.New()
	resp, err := client.R().Get(url)
	if err != nil {
		subLog.Error().Err(err).Msg("error when fetching icon")
		return []byte{}
	}

	if resp.StatusCode() >= 400 {
		subLog.Error().Int("StatusCode", resp.StatusCode()).Msg("error code received from server when fetching icon")
		return []byte{}
	}

	body := resp.Body()
	if err != nil {
		subLog.Error().Stack().Err(err).Msg("could not read response body when fetching icon")
		return []byte{}
	}

	return body
}

func FetchAssets(assetTypes []string, maxPages int) []*common.Asset {
	limit := rateLimit()
	assets := []*common.Asset{}
	pageNum := 1
	for _, assetType := range assetTypes {
		url := fmt.Sprintf("https://api.polygon.io/v3/reference/tickers?type=%s&market=stocks&active=true&sort=ticker&order=asc&limit=1000", assetType)
		subLog := log.With().Str("Url", url).Str("Source", "polygon.io").Logger()
		pageCnt := 1
		for {
			if pageCnt > maxPages {
				break
			}
			pageCnt++
			limit.Wait(context.Background())
			subLog.Info().Int("Page", pageNum).Msg("Loading page")
			pageNum++
			resp := fetchAssetPage(url)
			if resp.Status == "OK" {
				for _, asset := range resp.Results {
					newAsset := &common.Asset{
						Ticker:          asset.Ticker,
						Name:            asset.Name,
						PrimaryExchange: asset.PrimaryExchange,
						CompositeFigi:   asset.CompositeFigi,
						ShareClassFigi:  asset.ShareClassFigi,
						CIK:             asset.CIK,
						Source:          "api.polygon.io",
					}
					switch asset.Type {
					case "CS":
						newAsset.AssetType = common.CommonStock
					case "ETF":
						newAsset.AssetType = common.ETF
					case "ETN":
						newAsset.AssetType = common.ETN
					case "Fund":
						newAsset.AssetType = common.Fund
					}
					assets = append(assets, newAsset)
				}
				if resp.NextUrl == "" {
					break
				}
				url = resp.NextUrl
			} else {
				break
			}
		}
	}
	return assets
}

func fetchAssetPage(url string) PolygonAssetsResponse {
	// add url to log BEFORE the apikey is added in order not to expose a secret
	subLog := log.With().Str("Url", url).Str("Source", "polygon.io").Logger()
	// add apiKey
	url = fmt.Sprintf("%s&apiKey=%s", url, viper.GetString("polygon.token"))

	assetsResponse := PolygonAssetsResponse{}
	client := resty.New()

	resp, err := client.
		R().
		Get(url)

	if err != nil {
		subLog.Error().Err(err).Msg("error when fetching list of assets")
		return assetsResponse
	}

	if resp.StatusCode() >= 400 {
		subLog.Error().Int("StatusCode", resp.StatusCode()).Msg("error code received from server when fetching assets")
	}

	body := resp.Body()
	if err != nil {
		subLog.Error().Stack().Err(err).Msg("could not read response body when fetching assets")
		return assetsResponse
	}

	if err := json.Unmarshal(body, &assetsResponse); err != nil {
		subLog.Error().Stack().Err(err).Msg("could not unmarshal response body when fetching assets")
		return assetsResponse
	}

	if assetsResponse.Status != "OK" {
		subLog.Error().Str("PolygonStatus", assetsResponse.Status).Err(err).Msg("polygon status code not OK")
		return assetsResponse
	}

	return assetsResponse
}
