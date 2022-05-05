package common

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"os"
	"time"

	_ "image/jpeg"
	_ "image/png"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type AssetType string

const (
	CommonStock AssetType = "Common Stock"
	ETF         AssetType = "Exchange Traded Ffund"
	ETN         AssetType = "Exchange Traded Note"
	Fund        AssetType = "Closed-End Fund"
	MutualFund  AssetType = "Mutual Fund"
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
	LastUpdated          int64     `json:"last_updated" parquet:"name=last_update, type=INT64"`
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

	// TODO: Merge similar tickers

	return a
}

func ReadFromParquet(fn string) []*Asset {
	fr, err := local.NewLocalFileReader(fn)
	if err != nil {
		log.Error().Err(err).Msg("can't open file")
		return nil
	}

	pr, err := reader.NewParquetReader(fr, new(Asset), 4)
	if err != nil {
		log.Error().Err(err).Msg("can't create parquet reader")
		return nil
	}

	num := int(pr.GetNumRows())
	rec := make([]*Asset, num)
	if err = pr.Read(&rec); err != nil {
		log.Error().Err(err).Msg("parquet read error")
		return nil
	}

	pr.ReadStop()
	fr.Close()

	return rec
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

func SaveToDatabase(assets []*Asset, dsn string) error {
	log.Info().Msg("saving to database")
	conn, err := pgx.Connect(context.Background(), viper.GetString("DATABASE_URL"))
	if err != nil {
		log.Error().Err(err).Msg("could not connect to database")
	}
	defer conn.Close(context.Background())
	/*
		for _, quote := range quotes {
			_, err := conn.Exec(context.Background(),
				`INSERT INTO eod_v1 (
				"ticker",
				"composite_figi",
				"event_date",
				"open",
				"high",
				"low",
				"close",
				"volume",
				"dividend",
				"split_factor",
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
				$11
			) ON CONFLICT ON CONSTRAINT eod_v1_pkey
			DO UPDATE SET
				open = EXCLUDED.open,
				high = EXCLUDED.high,
				low = EXCLUDED.low,
				close = EXCLUDED.close,
				volume = EXCLUDED.volume,
				dividend = EXCLUDED.dividend,
				split_factor = EXCLUDED.split_factor,
				source = EXCLUDED.source;`,
				quote.Ticker, quote.CompositeFigi, quote.Date,
				quote.Open, quote.High, quote.Low, quote.Close, quote.Volume,
				quote.Dividend, quote.Split, "fred.stlouisfed.org")
			if err != nil {
				query := fmt.Sprintf(`INSERT INTO eod_v1 ("ticker", "composite_figi", "event_date", "open", "high", "low", "close", "volume", "dividend", "split_factor", "source") VALUES ('%s', '%s', '%s', %.5f, %.5f, %.5f, %.5f, %d, %.5f, %.5f, '%s') ON CONFLICT ON CONSTRAINT eod_v1_pkey DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume, dividend = EXCLUDED.dividend, split_factor = EXCLUDED.split_factor, source = EXCLUDED.source;`,
					quote.Ticker, quote.CompositeFigi, quote.Date,
					quote.Open, quote.High, quote.Low, quote.Close, quote.Volume,
					quote.Dividend, quote.Split, "fred.stlouisfed.org")
				log.Error().Err(err).Str("Query", query).Msg("error saving EOD quote to database")
			}
		}
	*/

	return nil
}
