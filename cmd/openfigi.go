// Copyright 2021
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"time"

	"github.com/penny-vault/import-tickers/common"
	"github.com/penny-vault/import-tickers/figi"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

func init() {
	rootCmd.AddCommand(openFigiCmd)
}

var openFigiCmd = &cobra.Command{
	Use:   "openfigi [ticker]",
	Short: "Lookup OpenFigi info for given ticker or for tickers with no figis in tickers.parquet",
	Long:  `Lookup OpenFigi info for given ticker or for tickers with no figis in tickers.parquet`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			// Search for FIGI's when the field is blank
			assets := common.ReadFromParquet(viper.GetString("parquet_file"))
			log.Info().Int("NumAssets", len(assets)).Msg("fetching missing figi's")

			currentTime := time.Now().Unix()

			figi.Enrich(assets)

			for _, asset := range assets {
				if asset.CompositeFigi == "" && asset.DelistingDate == "" {
					log.Info().Str("Ticker", asset.Ticker).Msg("asset has no composite figi")
				}
				if asset.LastUpdated > currentTime {
					log.Info().Str("Ticker", asset.Ticker).Str("CompositeFigi", asset.CompositeFigi).Msg("asset updated")
				}
			}

			finalAssets := make([]*common.Asset, 0, len(assets))
			for _, asset := range assets {
				if asset.CompositeFigi != "" {
					finalAssets = append(finalAssets, asset)
				}
			}

			common.SaveToParquet(finalAssets, viper.GetString("parquet_file"))
		} else {
			// lookup individual tickers
			dur := (time.Second * 6) / 25
			openFigiRate := rate.Every(dur)
			rateLimit := rate.NewLimiter(openFigiRate, 10)

			assets := make([]*common.Asset, len(args))
			for ii, ticker := range args {
				assets[ii] = &common.Asset{
					Ticker: ticker,
				}
			}

			figiResp := figi.LookupFigi(assets, rateLimit)
			for _, asset := range figiResp {
				assetFigi := figiResp[asset.Ticker]
				log.Info().
					Str("Ticker", asset.Ticker).
					Str("Name", assetFigi.Name).
					Str("SecurityType", assetFigi.SecurityType).
					Str("SecurityType2", assetFigi.SecurityType2).
					Str("Description", assetFigi.SecurityDescription).
					Str("CompositeFigi", assetFigi.CompositeFIGI).
					Str("ShareClassFigi", assetFigi.ShareClassFIGI).
					Str("MarketSector", assetFigi.MarketSector).
					Msg("found figi")
			}
		}
	},
}
