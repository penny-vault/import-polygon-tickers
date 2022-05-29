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
	"context"
	"time"

	"github.com/penny-vault/import-tickers/common"
	"github.com/penny-vault/import-tickers/yfinance"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var yfinanceLimit int

func init() {
	rootCmd.AddCommand(yfinanceCmd)

	yfinanceCmd.Flags().IntVar(&yfinanceLimit, "limit", 0, "only lookup N assets")
}

var yfinanceCmd = &cobra.Command{
	Use:   "yfinance [ticker]",
	Short: "Lookup yfinance info for given ticker or for tickers with no meta-data in tickers.parquet",
	Long:  `Lookup yfinance info for given ticker or for tickers with no meta-data in tickers.parquet`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			// Search for FIGI's when the field is blank
			assets := common.ReadAssetsFromParquet(viper.GetString("parquet_file"))
			log.Info().Int("NumAssets", len(assets)).Msg("fetching meta-data from yahoo")

			currentTime := time.Now().Unix()

			yfinance.Enrich(assets, yfinanceLimit)

			for _, asset := range assets {
				if asset.LastUpdated > currentTime {
					log.Info().
						Str("Ticker", asset.Ticker).
						Str("Name", asset.Name).
						Str("Description", asset.Description).
						Str("Industry", asset.Industry).
						Str("Sector", asset.Sector).
						Msg("updated")
				}
			}

			common.SaveToParquet(assets, viper.GetString("parquet_file"))
		} else {
			rateLimit := yfinance.RateLimit()

			assets := make([]*common.Asset, len(args))
			for ii, ticker := range args {
				assets[ii] = &common.Asset{
					Ticker: ticker,
				}
			}

			for _, asset := range assets {
				rateLimit.Wait(context.Background())
				yfinance.Download(asset)
				log.Info().
					Str("Ticker", asset.Ticker).
					Str("Name", asset.Name).
					Str("Description", asset.Description).
					Str("Industry", asset.Industry).
					Str("Sector", asset.Sector).
					Msg("update")
			}
		}
	},
}
