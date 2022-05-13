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
	"github.com/penny-vault/import-tickers/common"
	"github.com/penny-vault/import-tickers/polygon"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var maxPolyDetail int

func init() {
	rootCmd.AddCommand(polygonCmd)
	polygonCmd.Flags().IntVar(&maxPolyDetail, "max-polygon-detail", 0, "maximum polygon detail to fetch")
}

var polygonCmd = &cobra.Command{
	Use:   "polygon [ticker]",
	Short: "Lookup polygon details for given ticker or for tickers not recently reviewed in tickers.parquet",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			// Search for FIGI's when the field is blank
			assets := common.ReadFromParquet(viper.GetString("parquet_file"))
			log.Info().Int("NumAssets", len(assets)).Msg("fetching polygon details")
			polygon.EnrichDetail(assets, maxPolyDetail)
			common.SaveToParquet(assets, viper.GetString("parquet_file"))
		} else {
			assets := make([]*common.Asset, len(args))
			for ii, ticker := range args {
				assets[ii] = &common.Asset{
					Ticker: ticker,
				}
			}

			polygon.EnrichDetail(assets, 0)
			for _, asset := range assets {
				log.Info().
					Str("Ticker", asset.Ticker).
					Str("Name", asset.Name).
					Str("CompositeFigi", asset.CompositeFigi).
					Str("Description", asset.Description).
					Str("IconUrl", asset.IconUrl).
					Str("ShareClassFigi", asset.ShareClassFigi).
					Str("Sector", asset.Sector).
					Msg("updated asset")
			}
		}
	},
}
