package common

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"os"
	"strings"
	"time"

	_ "image/jpeg"
	_ "image/png"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type AssetType string

const (
	CommonStock   AssetType = "Common Stock"
	ETF           AssetType = "Exchange Traded Fund"
	ETN           AssetType = "Exchange Traded Note"
	ClosedEndFund AssetType = "Closed-End Fund"
	OpenEndFund   AssetType = "Mutual Fund"
	ADRC          AssetType = "American Depository Receipt Common"
	UnknownAsset  AssetType = "Unknown"
)

type Asset struct {
	Ticker               string    `json:"ticker" parquet:"name=ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Name                 string    `json:"Name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Description          string    `json:"description" parquet:"name=description, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PrimaryExchange      string    `json:"primary_exchange" parquet:"name=primary_exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetType            AssetType `json:"asset_type" parquet:"name=asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi        string    `json:"composite_figi" parquet:"name=composite_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ShareClassFigi       string    `json:"share_class_figi" parquet:"name=share_class_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CUSIP                string    `json:"cusip" parquet:"name=cusip, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ISIN                 string    `json:"isin" parquet:"name=isin, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CIK                  string    `json:"cik" parquet:"name=cik, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ListingDate          string    `json:"listing_date" parquet:"name=listing_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DelistingDate        string    `json:"delisting_date" parquet:"name=delisting_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Industry             string    `json:"industry" parquet:"name=industry, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sector               string    `json:"sector" parquet:"name=sector, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Icon                 []byte    `json:"icon"`
	IconUrl              string    `json:"icon_url" parquet:"name=icon_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CorporateUrl         string    `json:"corporate_url" parquet:"name=corporate_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	HeadquartersLocation string    `json:"headquarters_location" parquet:"name=headquarters_location, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SimilarTickers       []string  `json:"similar_tickers" parquet:"name=similar_tickers, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	PolygonDetailAge     int64     `json:"polygon_detail_age" parquet:"name=polygon_detail_age, type=INT64"`
	FidelityCusip        bool      `parquet:"name=fidelity_cusip, type=BOOLEAN"`
	LastUpdated          int64     `json:"last_updated" parquet:"name=last_update, type=INT64"`
	Source               string    `json:"source" parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type assetTmp struct {
	Ticker               string   `json:"ticker" parquet:"name=ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Name                 string   `json:"Name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Description          string   `json:"description" parquet:"name=description, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PrimaryExchange      string   `json:"primary_exchange" parquet:"name=primary_exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetType            string   `json:"asset_type" parquet:"name=asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi        string   `json:"composite_figi" parquet:"name=composite_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ShareClassFigi       string   `json:"share_class_figi" parquet:"name=share_class_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CUSIP                string   `json:"cusip" parquet:"name=cusip, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ISIN                 string   `json:"isin" parquet:"name=isin, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CIK                  string   `json:"cik" parquet:"name=cik, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ListingDate          string   `json:"listing_date" parquet:"name=listing_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DelistingDate        string   `json:"delisting_date" parquet:"name=delisting_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Industry             string   `json:"industry" parquet:"name=industry, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sector               string   `json:"sector" parquet:"name=sector, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	IconUrl              string   `json:"icon_url" parquet:"name=icon_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CorporateUrl         string   `json:"corporate_url" parquet:"name=corporate_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	HeadquartersLocation string   `json:"headquarters_location" parquet:"name=headquarters_location, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SimilarTickers       []string `json:"similar_tickers" parquet:"name=similar_tickers, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	PolygonDetailAge     int64    `json:"polygon_detail_age" parquet:"name=polygon_detail_age, type=INT64"`
	FidelityCusip        bool     `parquet:"name=fidelity_cusip, type=BOOLEAN"`
	LastUpdated          int64    `json:"last_updated" parquet:"name=last_update, type=INT64"`
	Source               string   `json:"source" parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func BuildAssetMap(assets []*Asset) map[string]*Asset {
	assetMap := make(map[string]*Asset, len(assets))
	for _, asset := range assets {
		assetMap[asset.Ticker] = asset
	}
	return assetMap
}

func CleanAssets(assets []*Asset) []*Asset {
	clean := make([]*Asset, 0, len(assets))
	for _, asset := range assets {
		if asset.CompositeFigi != "" && asset.AssetType != UnknownAsset {
			clean = append(clean, asset)
		}
	}
	return clean
}

func TrimWhiteSpace(assets []*Asset) {
	for _, asset := range assets {
		asset.Name = strings.TrimSpace(asset.Name)
		asset.Description = strings.TrimSpace(asset.Description)
		asset.CIK = strings.TrimSpace(asset.CIK)
		asset.CUSIP = strings.TrimSpace(asset.CUSIP)
		asset.Industry = strings.TrimSpace(asset.Industry)
		asset.Sector = strings.TrimSpace(asset.Sector)
		asset.ISIN = strings.TrimSpace(asset.ISIN)
	}
}

func MergeWithCurrent(assets []*Asset) []*Asset {
	mergedAssets := make([]*Asset, 0, len(assets))

	existingAssets := ReadFromParquet(viper.GetString("parquet_file"))
	log.Info().Int("NumAssets", len(existingAssets)).Msg("read parquet")

	assetMapTickerExisting := make(map[string]*Asset)
	assetMapTickerNew := make(map[string]*Asset)

	// build hash maps
	for _, asset := range existingAssets {
		// remove delisted tickers
		if asset.DelistingDate == "" {
			assetMapTickerExisting[asset.Ticker] = asset
		} else {
			log.Info().Object("Asset", asset).Msg("retired asset")
		}
	}

	for _, asset := range assets {
		if asset.DelistingDate == "" {
			assetMapTickerNew[asset.Ticker] = asset
		}
	}

	// enrich assets with existing database
	totalNew := 0
	for ii, asset := range assets {
		if asset.DelistingDate != "" {
			continue
		}
		// does the asset already exist?
		if origAsset, ok := assetMapTickerExisting[asset.Ticker]; ok {
			mergedAsset := MergeAsset(origAsset, asset)
			assets[ii] = mergedAsset
			mergedAssets = append(mergedAssets, mergedAsset)
		} else {
			// add new ticker to db
			mergedAssets = append(mergedAssets, asset)
			totalNew++
			asset.LastUpdated = time.Now().Unix()
		}
	}

	log.Info().Int("New", totalNew).Msg("merged assets")

	// mark assets not in the assetMapTickerNew as delisted
	for _, asset := range existingAssets {
		if _, ok := assetMapTickerNew[asset.Ticker]; !ok {
			log.Debug().Str("Ticker", asset.Ticker).Str("CompositeFigi", asset.CompositeFigi).Msg("asset de-listed")
			if asset.DelistingDate == "" {
				asset.DelistingDate = time.Now().Format("2006-01-02")
				asset.LastUpdated = time.Now().Unix()
			}
			mergedAssets = append(mergedAssets, asset)
		}
	}

	return mergedAssets
}

// Merge fields from b into a
func MergeAsset(a *Asset, b *Asset) *Asset {
	if a.Ticker != b.Ticker {
		log.Error().
			Str("a.Ticker", a.Ticker).
			Str("b.Ticker", b.Ticker).
			Msg("cannot merge assets with different tickers")
		return a
	}

	if a.AssetType != b.AssetType {
		log.Warn().
			Str("Ticker", a.Ticker).
			Str("CompositeFigi", a.CompositeFigi).
			Str("a.AssetType", string(a.AssetType)).
			Str("b.AssetType", string(b.AssetType)).
			Msg("asset types changed for ticker - ignoring change")
	}

	if a.AssetType == "" && b.AssetType != "" {
		a.AssetType = b.AssetType
	}

	if b.CIK != "" && a.CIK != b.CIK {
		a.CIK = b.CIK
		a.LastUpdated = time.Now().Unix()
	}

	if b.CUSIP != "" && a.CUSIP != b.CUSIP {
		a.CUSIP = b.CUSIP
		a.LastUpdated = time.Now().Unix()
	}

	if b.CompositeFigi != "" && a.CompositeFigi != b.CompositeFigi {
		a.CompositeFigi = b.CompositeFigi
		a.LastUpdated = time.Now().Unix()
	}

	if b.CorporateUrl != "" && a.CorporateUrl != b.CorporateUrl {
		a.CorporateUrl = b.CorporateUrl
		a.LastUpdated = time.Now().Unix()
	}

	if b.DelistingDate != "" && a.DelistingDate != b.DelistingDate {
		a.DelistingDate = b.DelistingDate
		a.LastUpdated = time.Now().Unix()
	}

	if b.Description != "" && a.Description != b.Description {
		a.Description = b.Description
		a.LastUpdated = time.Now().Unix()
	}

	if b.HeadquartersLocation != "" && a.HeadquartersLocation != b.HeadquartersLocation {
		a.HeadquartersLocation = b.HeadquartersLocation
		a.LastUpdated = time.Now().Unix()
	}

	if b.ISIN != "" && a.ISIN != b.ISIN {
		a.ISIN = b.ISIN
		a.LastUpdated = time.Now().Unix()
	}

	if b.IconUrl != "" && a.IconUrl != b.IconUrl {
		a.IconUrl = b.IconUrl
		a.LastUpdated = time.Now().Unix()
	}

	if b.Industry != "" && a.Industry != b.Industry {
		a.Industry = b.Industry
		a.LastUpdated = time.Now().Unix()
	}

	if b.ListingDate != "" && a.ListingDate != b.ListingDate {
		a.ListingDate = b.ListingDate
		a.LastUpdated = time.Now().Unix()
	}

	if b.Name != "" && a.Name != b.Name {
		a.Name = b.Name
		a.LastUpdated = time.Now().Unix()
	}

	if b.PrimaryExchange != "" && a.PrimaryExchange != b.PrimaryExchange {
		a.PrimaryExchange = b.PrimaryExchange
		a.LastUpdated = time.Now().Unix()
	}

	if b.Sector != "" && a.Sector != b.Sector {
		a.Sector = b.Sector
		a.LastUpdated = time.Now().Unix()
	}

	if b.ShareClassFigi != "" && a.ShareClassFigi != b.ShareClassFigi {
		a.ShareClassFigi = b.ShareClassFigi
		a.LastUpdated = time.Now().Unix()
	}

	if a.Source != b.Source {
		a.Source = b.Source
		a.LastUpdated = time.Now().Unix()
	}

	return a
}

func ReadFromParquet(fn string) []*Asset {
	log.Info().Str("FileName", fn).Msg("loading parquet file")
	fr, err := local.NewLocalFileReader(fn)
	if err != nil {
		log.Error().Err(err).Msg("can't open file")
		return nil
	}

	pr, err := reader.NewParquetReader(fr, new(assetTmp), 4)
	if err != nil {
		log.Error().Err(err).Msg("can't create parquet reader")
		return nil
	}

	num := int(pr.GetNumRows())
	rec := make([]*assetTmp, num)
	if err = pr.Read(&rec); err != nil {
		log.Error().Err(err).Msg("parquet read error")
		return nil
	}

	pr.ReadStop()
	fr.Close()

	assets := make([]*Asset, num)
	for ii, asset := range rec {
		assets[ii] = &Asset{
			Ticker:               asset.Ticker,
			Name:                 asset.Name,
			Description:          asset.Description,
			PrimaryExchange:      asset.PrimaryExchange,
			AssetType:            AssetType(asset.AssetType),
			CompositeFigi:        asset.CompositeFigi,
			ShareClassFigi:       asset.ShareClassFigi,
			CUSIP:                asset.CUSIP,
			ISIN:                 asset.ISIN,
			CIK:                  asset.CIK,
			ListingDate:          asset.ListingDate,
			DelistingDate:        asset.DelistingDate,
			Industry:             asset.Industry,
			Sector:               asset.Sector,
			IconUrl:              asset.IconUrl,
			CorporateUrl:         asset.CorporateUrl,
			HeadquartersLocation: asset.HeadquartersLocation,
			SimilarTickers:       asset.SimilarTickers,
			PolygonDetailAge:     asset.PolygonDetailAge,
			FidelityCusip:        asset.FidelityCusip,
			LastUpdated:          asset.LastUpdated,
		}
	}

	return assets
}

func SaveToParquet(records []*Asset, fn string) error {
	var err error

	fh, err := local.NewLocalFileWriter(fn)
	if err != nil {
		log.Error().Err(err).Str("FileName", fn).Msg("cannot create local file")
		return err
	}
	defer fh.Close()

	pw, err := writer.NewParquetWriter(fh, new(Asset), 4)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Parquet write failed")
		return err
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8k
	pw.CompressionType = parquet.CompressionCodec_GZIP

	for _, r := range records {
		if r.DelistingDate != "" {
			continue
		}
		if err = pw.Write(r); err != nil {
			log.Error().
				Err(err).
				Str("CompositeFigi", r.CompositeFigi).
				Msg("Parquet write failed for record")
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Error().Err(err).Msg("Parquet write failed")
		return err
	}

	log.Info().Int("NumRecords", len(records)).Msg("parquet write finished")
	return nil
}

// SaveIcons writes icon images to disk. Each icon is name <dirpath>/ticker.png|jpeg
func SaveIcons(assets []*Asset, dirpath string) {
	for _, asset := range assets {
		subLog := log.With().Str("Ticker", asset.Ticker).Logger()
		if len(asset.Icon) == 0 {
			continue
		}
		data := bytes.NewReader(asset.Icon)
		_, imType, err := image.Decode(data)
		if err != nil {
			subLog.Error().Err(err).Msg("failed to read image data")
			continue
		}

		os.WriteFile(fmt.Sprintf("%s/%s.%s", dirpath, asset.Ticker, imType), asset.Icon, 0666)
	}
}

func SaveToDatabase(assets []*Asset, dsn string) {
	log.Info().Msg("saving to database")
	conn, err := pgx.Connect(context.Background(), viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("could not connect to database")
		return
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(context.Background())
	if err != nil {
		log.Error().Err(err).Msg("could not begin transaction")
		return
	}

	// set all assets as inactive; active assets will be reset in subsequent step
	_, err = tx.Exec(context.Background(),
		`UPDATE assets SET active=False`)
	if err != nil {
		log.Error().Err(err).Msg("failed setting assets as inactive")
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
			log.Warn().Object("Asset", asset).Msg("asset source not set")
			asset.Source = "api.polygon.io"
		}

		_, err := tx.Exec(context.Background(),
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
				$17,
				$18,
				$19,
				$20
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
			listingDate,
			delistingDate,
			time.Unix(asset.LastUpdated, 0),
			asset.Source,
		)
		if err != nil {
			log.Error().Err(err).Object("Asset", asset).Msg("error saving asset to database")
			tx.Rollback(context.Background())
			return
		}
	}

	tx.Commit(context.Background())
}

func (asset *Asset) MarshalZerologObject(e *zerolog.Event) {
	e.Str("Ticker", asset.Ticker)
	e.Str("Name", asset.Name)
	e.Str("Description", asset.Description)
	e.Str("PrimaryExchange", asset.PrimaryExchange)
	e.Str("AssetType", string(asset.AssetType))
	e.Str("CompositeFigi", asset.CompositeFigi)
	e.Str("ShareClassFigi", asset.ShareClassFigi)
	e.Str("CUSIP", asset.CUSIP)
	e.Str("ISIN", asset.ISIN)
	e.Str("CIK", asset.CIK)
	e.Str("ListingDate", asset.ListingDate)
	e.Str("DelistingDate", asset.DelistingDate)
	e.Str("Industry", asset.Industry)
	e.Str("Sector", asset.Sector)
	e.Str("IconUrl", asset.IconUrl)
	e.Str("CorporateUrl", asset.CorporateUrl)
	e.Str("HeadquartersLocation", asset.HeadquartersLocation)
	e.Str("Source", asset.Source)
	e.Int64("PolygonDetailAge", asset.PolygonDetailAge)
	e.Int64("LastUpdate", asset.LastUpdated)
}
