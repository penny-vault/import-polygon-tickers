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

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "import-polygon",
	Short: "Download end-of-day quotes from polygon",
	Long:  `Download end-of-day quotes from polygon and save to penny-vault database`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().
			Strs("Exchanges", viper.GetStringSlice("exchanges")).
			Strs("AssetTypes", viper.GetStringSlice("asset_types")).
			Msg("loading tickers")

		/*
			assets := polygon.FetchTickers()
			assets = polygon.FilterExchange(assets, viper.GetStringSlice("exchanges"))
			assets = polygon.FilterAssetType(assets, viper.GetStringSlice("asset_types"))
			assets = polygon.FilterAge(assets, viper.GetDuration("max_age"))

			limit := viper.GetInt("limit")
			if limit > 0 {
				assets = assets[:limit]
			}

			quotes := polygon.FetchEodQuotes(assets)
			if viper.GetString("parquet_file") != "" {
				polygon.SaveToParquet(quotes, viper.GetString("parquet_file"))
			}

			if viper.GetString("database_url") != "" {
				polygon.SaveToDatabase(quotes, viper.GetString("database_url"))
			}
		*/
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

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is import-polygon-tickers.toml)")
	rootCmd.PersistentFlags().Bool("log.json", false, "print logs as json to stderr")
	viper.BindPFlag("log.json", rootCmd.PersistentFlags().Lookup("log.json"))

	// Local flags
	rootCmd.Flags().StringP("polygon-token", "t", "<not-set>", "polygon API key token")
	viper.BindPFlag("polygon_token", rootCmd.Flags().Lookup("polygon-token"))

	rootCmd.Flags().StringP("database-url", "d", "host=localhost port=5432", "DSN for database connection")
	viper.BindPFlag("database_url", rootCmd.Flags().Lookup("database-url"))

	rootCmd.Flags().Uint32P("limit", "l", 0, "limit results to N")
	viper.BindPFlag("limit", rootCmd.Flags().Lookup("limit"))

	rootCmd.Flags().StringArray("asset-types", []string{"CS", "PFD", "ETF", "FUND"}, "types of assets to download - defaults to all. Run list-asset-types to see options")
	viper.BindPFlag("asset_types", rootCmd.Flags().Lookup("asset-types"))

	rootCmd.Flags().StringArray("exchanges", []string{}, "list of ISO code exchanges to download from -- do list-exchange-codes for all possible options")
	viper.BindPFlag("exchanges", rootCmd.Flags().Lookup("exchanges"))

	rootCmd.Flags().Duration("max-age", 24*7*time.Hour, "maximum number of days stocks end date may be set too and still included")
	viper.BindPFlag("max_age", rootCmd.Flags().Lookup("max-age"))

	rootCmd.Flags().Int("polygon-rate-limit", 5, "polygon rate limit (items per second)")
	viper.BindPFlag("polygon_rate_limit", rootCmd.Flags().Lookup("polygon-rate-limit"))

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

		// Search config in home directory with name ".import-polygon-tickers" (without extension).
		viper.AddConfigPath("/etc/import-polygon/") // path to look for the config file in
		viper.AddConfigPath(fmt.Sprintf("%s/.import-polygon-tickers", home))
		viper.AddConfigPath(".")
		viper.SetConfigType("toml")
		viper.SetConfigName("import-polygon-tickers")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
