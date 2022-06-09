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

package common

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func SaveToDatabase(assets []*Asset) error {
	log.Info().Msg("saving to database")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("could not connect to database")
		return err
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not begin transaction")
		return err
	}

	// reset active, new, and updated flags
	_, err = tx.Exec(ctx,
		`UPDATE assets SET active=False, updated=False, new=False`)
	if err != nil {
		log.Error().Err(err).Msg("failed setting assets as inactive")
		tx.Rollback(ctx)
		return err
	}

	// update known assets
	for _, asset := range assets {
		var listingDate *string = nil
		if asset.ListingDate != "" {
			listingDate = &asset.ListingDate
		}
		var delistingDate *string = nil
		if asset.DelistingDate != "" {
			delistingDate = &asset.DelistingDate
		}

		if asset.Source == "" {
			asset.Source = "api.polygon.io"
			if asset.AssetType == MutualFund {
				asset.Source = "api.tiingo.com"
			}
		}

		_, err := tx.Exec(ctx,
			`INSERT INTO assets (
				"ticker",
				"asset_type",
				"cik",
				"composite_figi",
				"share_class_figi",
				"primary_exchange",
				"cusip",
				"isin",
				"active",
				"name",
				"description",
				"corporate_url",
				"sector",
				"industry",
				"logo_url",
				"similar_tickers",
				"new",
				"updated",
				"listed_utc",
				"delisted_utc",
				"last_updated_utc",
				"source"
			) VALUES (
				$1,
				$2,
				$3,
				$4,
				$5,
				$6,
				$7,
				$8,
				$9,
				$10,
				$11,
				$12,
				$13,
				$14,
				$15,
				$16,
				't',
				$17,
				$18,
				$19,
				$20,
				$21
			) ON CONFLICT ON CONSTRAINT assets_pkey
			DO UPDATE SET
				cik = EXCLUDED.cik,
				composite_figi = EXCLUDED.composite_figi,
				share_class_figi = EXCLUDED.share_class_figi,
				primary_exchange = EXCLUDED.primary_exchange,
				cusip = EXCLUDED.cusip,
				isin = EXCLUDED.isin,
				active = EXCLUDED.active,
				name = EXCLUDED.name,
				description = EXCLUDED.description,
				corporate_url = EXCLUDED.corporate_url,
				sector = EXCLUDED.sector,
				industry = EXCLUDED.industry,
				logo_url = EXCLUDED.logo_url,
				similar_tickers = EXCLUDED.similar_tickers,
				updated = EXCLUDED.updated,
				listed_utc = EXCLUDED.listed_utc,
				delisted_utc = EXCLUDED.delisted_utc,
				last_updated_utc = EXCLUDED.last_updated_utc,
				source = EXCLUDED.source
			;`,
			asset.Ticker,
			asset.AssetType,
			asset.CIK,
			asset.CompositeFigi,
			asset.ShareClassFigi,
			asset.PrimaryExchange,
			asset.CUSIP,
			asset.ISIN,
			asset.DelistingDate == "",
			asset.Name,
			asset.Description,
			asset.CorporateUrl,
			asset.Sector,
			asset.Industry,
			asset.IconUrl,
			asset.SimilarTickers,
			asset.Updated,
			listingDate,
			delistingDate,
			time.Unix(asset.LastUpdated, 0),
			asset.Source,
		)
		if err != nil {
			log.Error().Err(err).Object("Asset", asset).Msg("error saving asset to database")
			tx.Rollback(ctx)
			return err
		}
	}

	if err = tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("error commiting tx to database")
		return err
	}

	return nil
}
