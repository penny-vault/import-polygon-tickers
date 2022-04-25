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

	"github.com/penny-vault/import-polygon-tickers/polygon"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(assetTypesCmd)
}

var assetTypesCmd = &cobra.Command{
	Use:   "list-asset-types",
	Short: "List supported asset types",
	Run: func(cmd *cobra.Command, args []string) {
		supportedAssets := polygon.ListSupportedAssetTypes()
		for _, assetType := range supportedAssets {
			fmt.Printf("%s\t%s\n", assetType.Code, assetType.Description)
		}
	},
}
