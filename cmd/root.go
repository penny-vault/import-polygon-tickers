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
	"math"
	"os"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/penny-vault/import-tickers/common"
	"github.com/penny-vault/import-tickers/polygon"
	"github.com/penny-vault/import-tickers/yfinance"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

var cfgFile string
var printAssets bool
var skipFetchPolygon bool
var skipFetchPolygonDetail bool
var skipFetchTiingo bool
var skipFetchYahoo bool

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "import-polygon",
	Short: "Download end-of-day quotes from polygon",
	Long:  `Download end-of-day quotes from polygon and save to penny-vault database`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().
			Strs("AssetTypes", viper.GetStringSlice("asset_types")).
			Msg("loading tickers")

		// initialize polygon rate limiter
		dur := time.Duration(int64(time.Second) * 60 / viper.GetInt64("polygon_rate_limit"))
		polygonRate := rate.Every(dur)
		polygonRateLimiter := rate.NewLimiter(polygonRate, 2)

		// initialize yahoo rate limiter
		dur = time.Duration(int64(time.Second) * 60 / viper.GetInt64("yahoo_rate_limit"))
		yahooRate := rate.Every(dur)
		yahooRateLimiter := rate.NewLimiter(yahooRate, 2)

		limit := viper.GetInt("limit")
		maxPages := 25
		if limit > 0 {
			maxPages = int(math.Ceil(float64(limit) / 1000))
		}

		// Fetch base list of assets

		assets := []*common.Asset{}
		if !skipFetchPolygon {
			assets = polygon.FetchAssets(viper.GetStringSlice("asset_types"), maxPages, polygonRateLimiter)
		}

		if limit > 0 && limit < len(assets) {
			assets = assets[:limit]
		}

		// Enrich with call to Polygon Asset Details
		if !skipFetchPolygonDetail {
			bar := progressbar.Default(int64(len(assets)))
			for _, asset := range assets {
				bar.Add(1)
				polygon.FetchAssetDetail(asset, polygonRateLimiter)
			}
		}

		// Enrich with call to Yahoo Finance
		if !skipFetchYahoo {
			bar := progressbar.Default(int64(len(assets)))
			for _, asset := range assets {
				bar.Add(1)
				yfinance.Download(asset, yahooRateLimiter)
			}
		}

		if printAssets {
			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Ticker", "Name", "Composite FIGI", "Exchange", "Description", "SIC", "SIC Description", "Industry", "Sector"})
			for _, asset := range assets {
				t.AppendRow(table.Row{
					asset.Ticker, asset.Name, asset.CompositeFigi, asset.PrimaryExchange, asset.Description, asset.SICCode, asset.SICDescription, asset.Industry, asset.Sector,
				})
			}
			t.Render()
		}

		if viper.GetString("parquet_file") != "" {
			common.SaveToParquet(assets, viper.GetString("parquet_file"))
		}

		if viper.GetString("database_url") != "" {
			common.SaveToDatabase(assets, viper.GetString("database_url"))
		}

		common.SaveIcons(assets, ".")
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

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is import-tickers.toml)")
	rootCmd.PersistentFlags().Bool("log.json", false, "print logs as json to stderr")
	viper.BindPFlag("log.json", rootCmd.PersistentFlags().Lookup("log.json"))

	// Local flags
	rootCmd.Flags().BoolVar(&skipFetchPolygon, "skip-polygon", false, "do not fetch assets from polygon")
	rootCmd.Flags().BoolVar(&skipFetchPolygonDetail, "skip-polygon-detail", false, "do not fetch asset details from polygon")
	rootCmd.Flags().BoolVar(&skipFetchTiingo, "skip-tiingo", false, "do not fetch assets from tiingo")
	rootCmd.Flags().BoolVar(&skipFetchYahoo, "skip-yahoo", false, "do not fetch asset details from yahoo")

	rootCmd.Flags().StringP("polygon-token", "t", "<not-set>", "polygon API key token")
	viper.BindPFlag("polygon_token", rootCmd.Flags().Lookup("polygon-token"))

	rootCmd.Flags().BoolVar(&printAssets, "print", true, "Print assets to screen")

	rootCmd.Flags().StringP("database-url", "d", "host=localhost port=5432", "DSN for database connection")
	viper.BindPFlag("database_url", rootCmd.Flags().Lookup("database-url"))

	rootCmd.Flags().Uint32P("limit", "l", 0, "limit results to N")
	viper.BindPFlag("limit", rootCmd.Flags().Lookup("limit"))

	rootCmd.Flags().StringArray("asset-types", []string{"CS", "ETF", "MF"}, "types of assets to download. { CS = Common Stock, ETF = Exchange Traded Funds, MF = Mutual Funds}")
	viper.BindPFlag("asset_types", rootCmd.Flags().Lookup("asset-types"))

	rootCmd.Flags().Duration("max-age", 24*7*time.Hour, "maximum number of days stocks end date may be set too and still included")
	viper.BindPFlag("max_age", rootCmd.Flags().Lookup("max-age"))

	rootCmd.Flags().Int("polygon-rate-limit", 5, "polygon rate limit (items per minute)")
	viper.BindPFlag("polygon_rate_limit", rootCmd.Flags().Lookup("polygon-rate-limit"))

	rootCmd.Flags().Int("yahoo-rate-limit", 120, "yahoo rate limit (items per minute)")
	viper.BindPFlag("yahoo_rate_limit", rootCmd.Flags().Lookup("yahoo-rate-limit"))

	rootCmd.Flags().String("parquet-file", "", "save results to parquet")
	viper.BindPFlag("parquet_file", rootCmd.Flags().Lookup("parquet-file"))
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
		viper.AddConfigPath("/etc/import-polygon/") // path to look for the config file in
		viper.AddConfigPath(fmt.Sprintf("%s/.import-tickers", home))
		viper.AddConfigPath(".")
		viper.SetConfigType("toml")
		viper.SetConfigName("import-tickers")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
