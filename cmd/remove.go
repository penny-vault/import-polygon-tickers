// Copyright 2022
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
	"os"

	"github.com/penny-vault/import-tickers/backblaze"
	"github.com/penny-vault/import-tickers/common"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.AddCommand(removeCmd)
}

var removeCmd = &cobra.Command{
	Use:   "remove [tickers ...]",
	Args:  cobra.MinimumNArgs(1),
	Short: "Remove specified tickers from the tickers.parquet file",
	Run: func(cmd *cobra.Command, args []string) {
		backblaze.Download(viper.GetString("parquet_file"), viper.GetString("backblaze.bucket"))

		// Load from parquet
		parquetDb := viper.GetString("parquet_file")
		if parquetDb == "" {
			log.Error().Msg("parquet_file must be set for remove option")
			os.Exit(1)
		}
		assets := common.ReadAssetsFromParquet(parquetDb)

		// remove assets
		thinnedAssets := make([]*common.Asset, 0, len(assets))
		removed := 0
		for _, asset := range assets {
			toRemove := false
			for _, removeTicker := range args {
				if asset.Ticker == removeTicker {
					toRemove = true
				}
			}
			if !toRemove {
				thinnedAssets = append(thinnedAssets, asset)
			} else {
				removed++
			}
		}

		log.Info().Int("NumRemoved", removed).Msg("Removed assets")

		if viper.GetString("parquet_file") != "" {
			common.SaveToParquet(thinnedAssets, viper.GetString("parquet_file"))
		}

		if !viper.GetBool("backblaze.skip_upload") {
			backblaze.Upload(viper.GetString("parquet_file"), viper.GetString("backblaze.bucket"), ".")
		}
	},
}
