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
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().
			Strs("AssetTypes", viper.GetStringSlice("asset_types")).
			Msg("loading tickers")

		//backblaze.Download(viper.GetString("parquet_file"), viper.GetString("backblaze.bucket"))

		// Fetch base list of assets
		log.Info().Msg("fetching assets from polygon")
		assets := polygon.FetchAssets(viper.GetStringSlice("asset_types"), 25)

		// merge with asset database
		log.Info().Msg("reading existing assets and merging with those downloaded from polygon")
		assets = common.MergeWithCurrent(assets)

		// Enrich with call to Polygon Asset Details
		log.Info().Msg("fetching asset details from polygon")
		polygon.EnrichDetail(assets, 5)

		// Fetch MutualFund tickers from tiingo
		assets = tiingo.GetMutualFundTickers(assets)

		// Search for FIGI's when the field is blank
		log.Info().Msg("fetching missing figi's")
		figi.Enrich(assets)

		// cleanup assets
		assets = common.CleanAssets(assets)

		// Enrich with call to Yahoo Finance
		log.Info().Msg("fetching data from yahoo!")
		yfinance.Enrich(assets, 5)

		if viper.GetString("parquet_file") != "" {
			common.SaveToParquet(assets, viper.GetString("parquet_file"))
		}

		if viper.GetString("database.url") != "" {
			common.SaveToDatabase(assets, viper.GetString("database.url"))
		}

		backblaze.Upload(viper.GetString("parquet_file"), viper.GetString("backblaze.bucket"), ".")
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
	rootCmd.PersistentFlags().StringP("database-url", "d", "host=localhost port=5432", "DSN for database connection")
	viper.BindPFlag("database.url", rootCmd.PersistentFlags().Lookup("database-url"))
	rootCmd.PersistentFlags().String("parquet-file", "tickers.parquet", "save results to parquet")
	viper.BindPFlag("parquet_file", rootCmd.PersistentFlags().Lookup("parquet-file"))

	rootCmd.PersistentFlags().String("backblaze-application-id", "<not-set>", "backblaze application id")
	viper.BindPFlag("backblaze.application_id", rootCmd.PersistentFlags().Lookup("backblaze-application-id"))
	rootCmd.PersistentFlags().String("backblaze-application-key", "<not-set>", "backblaze application key")
	viper.BindPFlag("backblaze.application_key", rootCmd.PersistentFlags().Lookup("backblaze-application-key"))
	rootCmd.PersistentFlags().String("backblaze-bucket", "ticker-info", "backblaze bucket")
	viper.BindPFlag("backblaze.bucket", rootCmd.PersistentFlags().Lookup("backblaze-bucket"))

	// polygon
	rootCmd.PersistentFlags().String("polygon-token", "<not-set>", "polygon API key token")
	viper.BindPFlag("polygon.token", rootCmd.PersistentFlags().Lookup("polygon-token"))
	rootCmd.PersistentFlags().Int64("max-polygon-detail-age", 86400*365, "maximum number of seconds since last call to detail")
	viper.BindPFlag("polygon.detail_age", rootCmd.PersistentFlags().Lookup("max-polygon-detail-age"))
	rootCmd.PersistentFlags().Int("polygon-rate-limit", 4, "polygon rate limit (items per minute)")
	viper.BindPFlag("polygon.rate_limit", rootCmd.PersistentFlags().Lookup("polygon-rate-limit"))

	// openfigi
	rootCmd.PersistentFlags().String("openfigi-apikey", "<not-set>", "openfigi API key token")
	viper.BindPFlag("openfigi.apikey", rootCmd.PersistentFlags().Lookup("openfigi-apikey"))

	// Local flags
	rootCmd.Flags().IntVar(&maxPolygonDetail, "max-polygon-detail", 100, "maximum polygon detail to fetch")

	rootCmd.Flags().StringArray("asset-types", []string{"CS", "ETF", "ETN", "FUND", "MF"}, "types of assets to download. { CS = Common Stock, ETF = Exchange Traded Funds, ETN = Exchange Traded Note, FUND = Closed-end fund, MF = Mutual Funds}")
	viper.BindPFlag("asset_types", rootCmd.Flags().Lookup("asset-types"))

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
