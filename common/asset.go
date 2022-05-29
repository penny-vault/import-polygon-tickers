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

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pelletier/go-toml"
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
	CommonStock  AssetType = "Common Stock"
	ETF          AssetType = "Exchange Traded Fund"
	ETN          AssetType = "Exchange Traded Note"
	CEF          AssetType = "Closed-End Fund"
	MutualFund   AssetType = "Mutual Fund"
	ADRC         AssetType = "American Depository Receipt Common"
	FRED         AssetType = "FRED"
	UnknownAsset AssetType = "Unknown"
)

type tomlAssetContainer struct {
	Assets []*Asset
}

type Asset struct {
	Ticker               string    `json:"ticker" parquet:"name=ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Name                 string    `json:"Name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Description          string    `json:"description" parquet:"name=description, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PrimaryExchange      string    `json:"primary_exchange" toml:"primary_exchange" parquet:"name=primary_exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetType            AssetType `json:"asset_type" toml:"asset_type" parquet:"name=asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi        string    `json:"composite_figi" toml:"composite_figi" parquet:"name=composite_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ShareClassFigi       string    `json:"share_class_figi" toml:"share_class_figi" parquet:"name=share_class_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CUSIP                string    `json:"cusip" parquet:"name=cusip, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ISIN                 string    `json:"isin" parquet:"name=isin, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CIK                  string    `json:"cik" parquet:"name=cik, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ListingDate          string    `json:"listing_date" toml:"listing_date" parquet:"name=listing_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DelistingDate        string    `json:"delisting_date" toml:"delisting_date" parquet:"name=delisting_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Industry             string    `json:"industry" parquet:"name=industry, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sector               string    `json:"sector" parquet:"name=sector, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Icon                 []byte    `json:"icon"`
	IconUrl              string    `json:"icon_url" toml:"icon_url" parquet:"name=icon_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CorporateUrl         string    `json:"corporate_url" toml:"corporate_url" parquet:"name=corporate_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	HeadquartersLocation string    `json:"headquarters_location" toml:"headquarters_location" parquet:"name=headquarters_location, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SimilarTickers       []string  `json:"similar_tickers" toml:"similar_tickers" parquet:"name=similar_tickers, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	PolygonDetailAge     int64     `json:"polygon_detail_age" parquet:"name=polygon_detail_age, type=INT64"`
	FidelityCusip        bool      `parquet:"name=fidelity_cusip, type=BOOLEAN"`

	Updated     bool
	LastUpdated int64  `json:"last_updated" parquet:"name=last_update, type=INT64"`
	Source      string `json:"source" parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
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

	Updated     bool
	LastUpdated int64  `json:"last_updated" parquet:"name=last_update, type=INT64"`
	Source      string `json:"source" parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// BuildAssetMap creates a map[string]*Asset hashmap where the ticker is the key
func BuildAssetMap(assets []*Asset) map[string]*Asset {
	assetMap := make(map[string]*Asset, len(assets))
	for _, asset := range assets {
		assetMap[asset.Ticker] = asset
	}
	return assetMap
}

// CleanAssets remove assets that have no composite figi or have an unknown asset type
func CleanAssets(assets []*Asset) []*Asset {
	clean := make([]*Asset, 0, len(assets))
	for _, asset := range assets {
		if asset.CompositeFigi != "" && asset.AssetType != UnknownAsset {
			clean = append(clean, asset)
		}
	}
	return clean
}

// TrimeWhiteSpace removes leading and trailing whitespace in selected fields of the asset
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

// FilterMixedCase removes assets that have mixed-case tickers
func FilterMixedCase(assets []*Asset) []*Asset {
	newAssets := make([]*Asset, 0, len(assets))
	for _, asset := range assets {
		if strings.ToUpper(asset.Ticker) == asset.Ticker {
			newAssets = append(newAssets, asset)
		}
	}
	return newAssets
}

// ReadAssetsFromToml reads assets stored as TOML from the file `fn`
func ReadAssetsFromToml(fn string) []*Asset {
	var assetContainer tomlAssetContainer

	doc, err := os.ReadFile(fn)
	if err != nil {
		log.Error().Err(err).Msg("reading TOML asset file failed")
		return []*Asset{}
	}

	err = toml.Unmarshal([]byte(doc), &assetContainer)
	if err != nil {
		log.Error().Err(err).Msg("parsing TOML asset file failed")
		return []*Asset{}
	}

	return assetContainer.Assets
}

func RemoveDelistedAssets(assets []*Asset) []*Asset {
	existingAssets := make([]*Asset, 0, len(assets))
	for _, asset := range assets {
		// remove delisted tickers
		if asset.DelistingDate == "" {
			existingAssets = append(existingAssets, asset)
		} else {
			log.Info().Object("Asset", asset).Msg("retired asset")
		}
	}
	return existingAssets
}

// SubtractAssets returns the set of assets in a but not b
func SubtractAssets(a []*Asset, b []*Asset) (sub []*Asset) {
	sub = make([]*Asset, 0, len(a))
	bAssetMap := BuildAssetMap(b)
	for _, aAsset := range a {
		if _, ok := bAssetMap[aAsset.Ticker]; !ok {
			//			log.Info().Str("a.Ticker", aAsset.Ticker).Msg("asset not in b")
			sub = append(sub, aAsset)
		}
	}
	return
}

// MergeAssetList combines assets from `first` and `second`. Assets in `first` are given preference to `second`
func MergeAssetList(first []*Asset, second []*Asset) (combinedAssets []*Asset, firstOnly []*Asset, secondOnly []*Asset) {
	combinedAssets = make([]*Asset, 0, len(first)+len(second))
	firstOnly = make([]*Asset, 0, len(first))
	secondOnly = make([]*Asset, 0, len(second))

	// build hash maps
	firstAssetMap := BuildAssetMap(first)
	secondAssetMap := BuildAssetMap(second)

	// add items of second to first
	for _, asset := range second {
		// does the asset already exist?
		if origAsset, ok := firstAssetMap[asset.Ticker]; ok {
			mergedAsset := MergeAsset(origAsset, asset)
			combinedAssets = append(combinedAssets, mergedAsset)
		} else {
			// add new ticker to db
			secondOnly = append(secondOnly, asset)
			combinedAssets = append(combinedAssets, asset)
		}
	}

	// find assets that are only in first
	for _, asset := range first {
		if _, ok := secondAssetMap[asset.Ticker]; !ok {
			firstOnly = append(firstOnly, asset)
			combinedAssets = append(combinedAssets, asset)
		}
	}

	return
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

	if a.AssetType == "" && b.AssetType != "" {
		a.AssetType = b.AssetType
	}

	if b.CIK != "" && a.CIK != b.CIK {
		a.CIK = b.CIK
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.CUSIP != "" && a.CUSIP != b.CUSIP {
		a.CUSIP = b.CUSIP
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.CompositeFigi != "" && a.CompositeFigi != b.CompositeFigi {
		a.CompositeFigi = b.CompositeFigi
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.CorporateUrl != "" && a.CorporateUrl != b.CorporateUrl {
		a.CorporateUrl = b.CorporateUrl
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.DelistingDate != "" && a.DelistingDate != b.DelistingDate {
		a.DelistingDate = b.DelistingDate
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.Description != "" && a.Description != b.Description {
		a.Description = b.Description
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.HeadquartersLocation != "" && a.HeadquartersLocation != b.HeadquartersLocation {
		a.HeadquartersLocation = b.HeadquartersLocation
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.ISIN != "" && a.ISIN != b.ISIN {
		a.ISIN = b.ISIN
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.IconUrl != "" && a.IconUrl != b.IconUrl {
		a.IconUrl = b.IconUrl
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.Industry != "" && a.Industry != b.Industry {
		a.Industry = b.Industry
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.ListingDate != "" && a.ListingDate != b.ListingDate {
		a.ListingDate = b.ListingDate
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.Name != "" && a.Name != b.Name {
		a.Name = b.Name
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.PrimaryExchange != "" && a.PrimaryExchange != b.PrimaryExchange {
		a.PrimaryExchange = b.PrimaryExchange
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.Sector != "" && a.Sector != b.Sector {
		a.Sector = b.Sector
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	if b.ShareClassFigi != "" && a.ShareClassFigi != b.ShareClassFigi {
		a.ShareClassFigi = b.ShareClassFigi
		a.Updated = true
		a.LastUpdated = time.Now().Unix()
	}

	return a
}

func ReadAssetsFromParquet(fn string) []*Asset {
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

// ActiveAssetsFromDatabase loads all active assets from the database
func ActiveAssetsFromDatabase() (assets []*Asset) {
	ctx := context.Background()
	assets = make([]*Asset, 0, 50000)

	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("could not connect to database")
		return
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `SELECT ticker, name, description,
primary_exchange, asset_type, composite_figi, share_class_figi, cusip,
isin, cik, listed_utc, industry, sector, logo_url,
corporate_url, similar_tickers, source FROM assets WHERE active='t'`)
	if err != nil {
		log.Error().Err(err).Msg("error querying database")
	}

	for rows.Next() {
		asset := Asset{}
		var listingDate pgtype.Timestamp
		var assetType string
		err = rows.Scan(&asset.Ticker, &asset.Name, &asset.Description,
			&asset.PrimaryExchange, &assetType, &asset.CompositeFigi,
			&asset.ShareClassFigi, &asset.CUSIP, &asset.ISIN,
			&asset.CIK, &listingDate,
			&asset.Industry, &asset.Sector, &asset.IconUrl,
			&asset.CorporateUrl, &asset.SimilarTickers, &asset.Source)
		if err != nil {
			log.Error().Err(err).Msg("error scanning row into asset structure")
		}
		asset.AssetType = AssetType(assetType)
		if listingDate.Status == pgtype.Present {
			asset.ListingDate = listingDate.Time.Format("2006-01-02")
		}
		assets = append(assets, &asset)
	}

	return
}

func SaveToDatabase(assets []*Asset) {
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

	// reset active, new, and updated flags
	_, err = tx.Exec(context.Background(),
		`UPDATE assets SET active=False, updated=False, new=False`)
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

// LogSummary logs statistics about each signficant asset change
func LogSummary(assets []*Asset) {
	nyc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Error().Err(err).Msg("could not load timezone")
	}

	now := time.Now().In(nyc)
	changedAge := time.Duration(7 * time.Minute)

	// Changed Assets
	for _, asset := range assets {
		lastUpdated := time.Unix(asset.LastUpdated, 0).In(nyc)
		if now.Sub(lastUpdated) > changedAge {
			log.Info().Object("Asset", asset).Msg("changed")
		}
	}
}
