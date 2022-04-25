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
	"fmt"
	"strings"

	"github.com/penny-vault/import-polygon-tickers/polygon"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(exchangeCodesCmd)
}

var exchangeCodesCmd = &cobra.Command{
	Use:   "list-exchange-codes",
	Short: "List ISO exchange codes",
	Long: `Print supported ISO exchange codes from:
https://www.iso20022.org/market-identifier-codes`,
	Run: func(cmd *cobra.Command, args []string) {
		exchangeCodes := polygon.ListExchangeCodes()
		for _, code := range exchangeCodes {
			if code.ISOCountryCode == "US" && code.Status == "ACTIVE" {
				fmt.Printf("%s\t%s\t\t%s\n", code.Mic, code.Name, strings.ToLower(code.Website))
			}
		}
	},
}
