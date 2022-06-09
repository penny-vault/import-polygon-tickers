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
package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/penny-vault/import-tickers/backblaze"
	"github.com/penny-vault/import-tickers/common"
	"github.com/penny-vault/import-tickers/figi"
	"github.com/penny-vault/import-tickers/polygon"
	"github.com/penny-vault/import-tickers/tiingo"
	"github.com/penny-vault/import-tickers/yfinance"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string
var maxPolygonDetail int
var maxPolygonDetailAge int64

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "import-tickers",
	Short: "Download tradeable assets from polygon, tiingo, and Yahoo! finance",
	Long: `Download tradeable assets from polygon, tiingo, and Yahoo!
and save to penny-vault database`,
	Run: func(cmd *cobra.Command, args []string) {
		nyc, err := time.LoadLocation("America/New_York")
		if err != nil {
			log.Error().Err(err).Msg("could not load timezone")
			os.Exit(1)
		}

		log.Info().
			Bool("SaveDB", viper.GetBool("database.save")).
			Bool("Backbalze.SkipUpload", viper.GetBool("backblaze.skip_upload")).
			Str("TickerDB", viper.GetString("parquet_file")).
			Msg("loading tickers")

		backblaze.Download(viper.GetString("parquet_file"), viper.GetString("backblaze.bucket"))

		// Fetch base list of assets
		log.Info().Msg("fetching assets from polygon")
		polygonAssets, err := polygon.FetchAssets(25)
		if err != nil {
			log.Error().Msg("exiting due to error downloading polygon assets")
			os.Exit(common.EXIT_CODE_POLYGON)
		}

		if len(polygonAssets) < viper.GetInt("polygon.min_assets") {
			log.Error().Int("NumAssets", len(polygonAssets)).Int("MinRequired", viper.GetInt("polygon.min_assets")).Msg("not enough polygon assets were downloaded - exiting")
			os.Exit(common.EXIT_CODE_ASSET_COUNT_OUT_OF_RANGE)
		}

		// Fetch MutualFund tickers from tiingo
		tiingoAssets := tiingo.FetchAssets()

		if len(tiingoAssets) < viper.GetInt("tiingo.min_assets") {
			log.Error().Int("NumAssets", len(tiingoAssets)).Int("MinRequired", viper.GetInt("tiingo.min_assets")).Msg("not enough tiingo assets were downloaded - exiting")
			os.Exit(common.EXIT_CODE_ASSET_COUNT_OUT_OF_RANGE)
		}

		// Merge polygon and tiingo lists
		mergedAssets, _, _ := common.MergeAssetList(polygonAssets, tiingoAssets)
		log.Info().Int("Num", len(mergedAssets)).Msg("polygon + tiingo")

		// Add tickers from file
		staticAssetsFn := viper.GetString("static_assets_fn")
		if staticAssetsFn != "" {
			tomlAssets := common.ReadAssetsFromToml(staticAssetsFn)
			log.Info().Int("Num", len(tomlAssets)).Str("FileName", staticAssetsFn).Msg("Read static assets from TOML file")
			mergedAssets, _, _ = common.MergeAssetList(mergedAssets, tomlAssets)
		}

		// blacklisted assets
		blacklistFn := viper.GetString("blacklist_fn")
		if blacklistFn != "" {
			blacklisted := common.ReadAssetsFromToml(blacklistFn)
			mergedAssets = common.RemoveAssets(mergedAssets, blacklisted)
		}

		// Load from parquet
		parquetDb := viper.GetString("parquet_file")
		if parquetDb != "" {
			parquetAssets := common.ReadAssetsFromParquet(parquetDb)
			log.Info().Int("NumAssets", len(parquetAssets)).Msg("read existing assets from parquet")

			// remove delisted assets
			parquetAssets = common.RemoveDelistedAssets(parquetAssets)

			var first []*common.Asset
			var second []*common.Asset
			mergedAssets, first, second = common.MergeAssetList(parquetAssets, mergedAssets)

			log.Info().Int("InParquetOnly", len(first)).Int("NewlyDownloaded", len(second)).Int("Total", len(mergedAssets)).Msg("merge with parquet")

			// mark items only in first as delisted
			for _, asset := range first {
				asset.DelistingDate = time.Now().In(nyc).Format("2006-01-02")
			}

			// mark items only in second as updated and set listing date if it's empty
			for _, asset := range second {
				asset.LastUpdated = time.Now().In(nyc).Unix()
				if asset.ListingDate == "" {
					asset.ListingDate = time.Now().In(nyc).Format("2006-01-02")
				}
			}
		}

		// Enrich with call to Polygon Asset Details
		log.Info().Msg("fetching asset details from polygon")
		polygon.EnrichDetail(mergedAssets, 5)

		// Search for FIGI's when the field is blank
		log.Info().Msg("fetching missing figi's")
		figi.Enrich(mergedAssets)

		// cleanup assets
		beforeCleanCnt := len(mergedAssets)
		mergedAssets = common.CleanAssets(mergedAssets)
		afterCleanCnt := len(mergedAssets)
		log.Debug().Int("RemovedAssetCount", beforeCleanCnt-afterCleanCnt).Msg("Removed assets with no FIGI or Asset Type")
		common.TrimWhiteSpace(mergedAssets)

		// Enrich with call to Yahoo Finance
		log.Info().Msg("fetching data from yahoo!")
		yfinance.Enrich(mergedAssets, 5)

		// Prune multi-case assets
		beforeFilterCnt := len(mergedAssets)
		mergedAssets = common.FilterMixedCase(mergedAssets)
		afterFilterCnt := len(mergedAssets)
		log.Debug().Int("RemovedAssetsCount", beforeFilterCnt-afterFilterCnt).Msg("filtered assets with mixed-case tickers")

		if viper.GetString("database.url") != "" {
			// Compare against assets currently in DB to find what is getting removed
			assetsDb := common.ActiveAssetsFromDatabase()
			removedAssets := common.SubtractAssets(assetsDb, mergedAssets)
			log.Info().Int("NumAssetsRemoved", len(removedAssets)).Msg("found delisted assets")

			// Check how many assets are marked for removal
			// this is a safety valve to not delete assets because a
			// service goes down
			numRemoved := len(removedAssets)
			for _, asset := range mergedAssets {
				if asset.DelistingDate != "" {
					numRemoved++
				}
			}
			if numRemoved > viper.GetInt("max_removed_count") {
				log.Error().Int("MaxAllowed", viper.GetInt("max_removed_count")).Int("Actual", numRemoved).Msg("too many assets removed - bailing")
				os.Exit(common.EXIT_CODE_ASSET_COUNT_OUT_OF_RANGE)
			}

			// mark removed assets so statistics are correctly calculated
			for _, asset := range removedAssets {
				asset.DelistingDate = time.Now().In(nyc).Format("2006-01-02")
				asset.LastUpdated = time.Now().In(nyc).Unix()
				asset.Updated = true
				asset.UpdateReason = "asset delisted"
				mergedAssets = append(mergedAssets, asset)
			}

			common.LogSummary(mergedAssets)

			if viper.GetBool("database.save") {
				if err = common.SaveToDatabase(mergedAssets); err != nil {
					os.Exit(common.EXIT_CODE_DATABASE_ERROR)
				}
			}
		}

		if viper.GetString("parquet_file") != "" {
			common.SaveToParquet(mergedAssets, viper.GetString("parquet_file"))
		}

		if !viper.GetBool("backblaze.skip_upload") {
			backblaze.Upload(viper.GetString("parquet_file"), viper.GetString("backblaze.bucket"), ".")
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	cobra.OnInitialize(initLog)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is import-tickers.toml)")
	rootCmd.PersistentFlags().Bool("log-json", false, "print logs as json to stderr")
	viper.BindPFlag("log.json", rootCmd.PersistentFlags().Lookup("log-json"))
	rootCmd.PersistentFlags().Bool("hide-progress", false, "hide progress bar")
	viper.BindPFlag("display.hide_progress", rootCmd.PersistentFlags().Lookup("hide-progress"))

	rootCmd.PersistentFlags().StringP("database-url", "d", "host=localhost port=5432", "DSN for database connection")
	viper.BindPFlag("database.url", rootCmd.PersistentFlags().Lookup("database-url"))
	rootCmd.PersistentFlags().Bool("database-save", false, "save assets to database")
	viper.BindPFlag("database.save", rootCmd.PersistentFlags().Lookup("database-save"))

	rootCmd.PersistentFlags().String("parquet-file", "tickers.parquet", "save results to parquet")
	viper.BindPFlag("parquet_file", rootCmd.PersistentFlags().Lookup("parquet-file"))

	rootCmd.PersistentFlags().Int("max-removed-count", 25, "maximum number of assets that can be removed per run; this is a safety feature in-case something goes wrong to prevent the database from getting hosed up")
	viper.BindPFlag("max_removed_count", rootCmd.PersistentFlags().Lookup("max-removed-count"))

	// static assets
	rootCmd.PersistentFlags().String("static-assets-fn", "", "load additional assets from the specified TOML file")
	viper.BindPFlag("static_assets_fn", rootCmd.PersistentFlags().Lookup("static-assets-fn"))

	// blacklisted assets
	rootCmd.PersistentFlags().String("blacklist-fn", "", "load additional assets from the specified TOML file")
	viper.BindPFlag("blackist_fn", rootCmd.PersistentFlags().Lookup("blacklist-fn"))

	// backblaze
	rootCmd.PersistentFlags().String("backblaze-application-id", "<not-set>", "backblaze application id")
	viper.BindPFlag("backblaze.application_id", rootCmd.PersistentFlags().Lookup("backblaze-application-id"))
	rootCmd.PersistentFlags().String("backblaze-application-key", "<not-set>", "backblaze application key")
	viper.BindPFlag("backblaze.application_key", rootCmd.PersistentFlags().Lookup("backblaze-application-key"))
	rootCmd.PersistentFlags().String("backblaze-bucket", "ticker-info", "backblaze bucket")
	viper.BindPFlag("backblaze.bucket", rootCmd.PersistentFlags().Lookup("backblaze-bucket"))
	rootCmd.PersistentFlags().Bool("backblaze-skip-upload", false, "skip backblaze upload")
	viper.BindPFlag("backblaze.skip_upload", rootCmd.PersistentFlags().Lookup("backblaze-skip-upload"))

	// polygon
	rootCmd.PersistentFlags().String("polygon-token", "<not-set>", "polygon API key token")
	viper.BindPFlag("polygon.token", rootCmd.PersistentFlags().Lookup("polygon-token"))
	rootCmd.PersistentFlags().Int64("max-polygon-detail-age", 86400*365, "maximum number of seconds since last call to detail")
	viper.BindPFlag("polygon.detail_age", rootCmd.PersistentFlags().Lookup("max-polygon-detail-age"))
	rootCmd.PersistentFlags().Int("polygon-rate-limit", 4, "polygon rate limit (items per minute)")
	viper.BindPFlag("polygon.rate_limit", rootCmd.PersistentFlags().Lookup("polygon-rate-limit"))
	rootCmd.PersistentFlags().Int("polygon-min-assets", 4000, "minimum number of assets expected from polygon")
	viper.BindPFlag("polygon.min_assets", rootCmd.PersistentFlags().Lookup("polygon-min-assets"))

	// tiingo
	rootCmd.PersistentFlags().Int("tiingo-min-assets", 15000, "minimum number of assets expected from tiingo")
	viper.BindPFlag("tiingo.min_assets", rootCmd.PersistentFlags().Lookup("tiingo-min-assets"))

	// openfigi
	rootCmd.PersistentFlags().String("openfigi-apikey", "<not-set>", "openfigi API key token")
	viper.BindPFlag("openfigi.apikey", rootCmd.PersistentFlags().Lookup("openfigi-apikey"))

	// Local flags
	rootCmd.Flags().IntVar(&maxPolygonDetail, "max-polygon-detail", 100, "maximum polygon detail to fetch")

	rootCmd.Flags().Duration("max-age", 24*7*time.Hour, "maximum number of days stocks end date may be set too and still included")
	viper.BindPFlag("max_age", rootCmd.Flags().Lookup("max-age"))

	rootCmd.Flags().Int("yahoo-rate-limit", 120, "yahoo rate limit (items per minute)")
	viper.BindPFlag("yahoo.rate_limit", rootCmd.Flags().Lookup("yahoo-rate-limit"))
}

func initLog() {
	if !viper.GetBool("log.json") {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".import-tickers" (without extension).
		viper.AddConfigPath("/etc/") // path to look for the config file in
		viper.AddConfigPath(fmt.Sprintf("%s/.config", home))
		viper.AddConfigPath(".")
		viper.SetConfigType("toml")
		viper.SetConfigName("import-tickers")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Debug().Str("ConfigFile", viper.ConfigFileUsed()).Msg("Loaded config file")
	} else {
		log.Error().Err(err).Msg("error reading config file")
		os.Exit(1)
	}
}
