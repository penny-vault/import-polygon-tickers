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
package polygon

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/ratelimit"
)

type Eod struct {
	Date          string  `json:"date" parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Ticker        string  `json:"ticker" parquet:"name=ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Exchange      string  `json:"exchange" parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetType     string  `json:"assetType" parquet:"name=assetType, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi string  `json:"compositeFigi" parquet:"name=compositeFigi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Open          float32 `json:"open" parquet:"name=open, type=FLOAT"`
	High          float32 `json:"high" parquet:"name=high, type=FLOAT"`
	Low           float32 `json:"low" parquet:"name=low, type=FLOAT"`
	Close         float32 `json:"close" parquet:"name=close, type=FLOAT"`
	Volume        int64   `json:"volume" parquet:"name=volume, type=INT64, convertedtype=INT_64"`
	Dividend      float32 `json:"divCash" parquet:"name=dividend, type=FLOAT"`
	Split         float32 `json:"splitFactor" parquet:"name=split, type=FLOAT"`
}

type Asset struct {
	CompositeFigi string `json:"compositeFigi"`
	Ticker        string `json:"ticker" csv:"ticker"`
	Exchange      string `json:"exchange" csv:"exchange"`
	AssetType     string `json:"assetType" csv:"assetType"`
	PriceCurrency string `json:"priceCurrency" csv:"priceCurrency"`
	StartDate     string `json:"startDate" csv:"startDate"`
	EndDate       string `json:"endDate" csv:"endDate"`
}

func readZipFile(zf *zip.File) ([]byte, error) {
	f, err := zf.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ioutil.ReadAll(f)
}

func SaveToParquet(records []*Eod, fn string) error {
	var err error

	fh, err := local.NewLocalFileWriter(fn)
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Str("FileName", fn).Msg("cannot create local file")
		return err
	}
	defer fh.Close()

	pw, err := writer.NewParquetWriter(fh, new(Eod), 4)
	if err != nil {
		log.Error().
			Str("OriginalError", err.Error()).
			Msg("Parquet write failed")
		return err
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8k
	pw.CompressionType = parquet.CompressionCodec_GZIP

	for _, r := range records {
		if err = pw.Write(r); err != nil {
			log.Error().
				Str("OriginalError", err.Error()).
				Str("EventDate", r.Date).Str("Ticker", r.Ticker).
				Str("CompositeFigi", r.CompositeFigi).
				Msg("Parquet write failed for record")
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("Parquet write failed")
		return err
	}

	log.Info().Int("NumRecords", len(records)).Msg("Parquet write finished")
	return nil
}

func FetchEodQuotes(assets []*Asset) []*Eod {
	// polygon rate limits
	limit := ratelimit.New(viper.GetInt("polygon_rate_limit"))

	quotes := []*Eod{}
	client := resty.New()
	startDate := time.Now().Add(-7 * 24 * time.Hour)
	startDateStr := startDate.Format("2006-01-02")

	bar := progressbar.Default(int64(len(assets)))
	for _, asset := range assets {
		bar.Add(1)
		limit.Take()
		url := fmt.Sprintf("https://api.tiingo.com/tiingo/daily/%s/prices?startDate=%s&token=%s", asset.Ticker, startDateStr, viper.Get("polygon_token"))
		resp, err := client.
			R().
			SetHeader("Accept", "application/json").
			Get(url)
		if err != nil {
			log.Error().Str("OriginalError", err.Error()).Str("Url", url).Msg("error when requesting eod quote")
		}
		if resp.StatusCode() >= 400 {
			log.Error().Int("StatusCode", resp.StatusCode()).Str("Url", url).Bytes("Body", resp.Body()).Msg("error when requesting eod quote")
		}
		data := resp.Body()
		var quote []*Eod
		if err = json.Unmarshal(data, &quote); err != nil {
			log.Error().Str("OriginalError", err.Error()).Msg("could not unmarshal json")
		} else {
			for _, q := range quote {
				q.Ticker = asset.Ticker
				q.Exchange = asset.Exchange
				q.AssetType = asset.AssetType
				q.CompositeFigi = asset.CompositeFigi
				quotes = append(quotes, q)
			}
		}
	}

	return quotes
}

// DownloadTickers fetches a list of supported tickers from Polygon
func FetchTickers() []*Asset {
	tickerUrl := "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
	client := resty.New()
	assets := []*Asset{}

	resp, err := client.
		R().
		Get(tickerUrl)
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to download tickers")
		return assets
	}
	if resp.StatusCode() >= 400 {
		log.Error().Int("StatusCode", resp.StatusCode()).Str("Url", tickerUrl).Bytes("Body", resp.Body()).Msg("error when requesting eod quote")
		return assets
	}

	// unzip downloaded data
	body := resp.Body()
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("could not read response body when downloading tickers")
		return assets
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to read tickers zip file")
		return assets
	}

	// Read all the files from zip archive
	var tickerCsvBytes []byte
	if len(zipReader.File) == 0 {
		log.Error().Msg("no files contained in received zip file")
		return assets
	}

	zipFile := zipReader.File[0]
	tickerCsvBytes, err = readZipFile(zipFile)
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to read ticker csv from zip")
		return assets
	}

	if err := gocsv.UnmarshalBytes(tickerCsvBytes, &assets); err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to unmarshal csv")
		return assets
	}

	return assets
}

// FilterExchange removes assets that are not traded on one of the listed exchanges
func FilterExchange(assets []*Asset, exchanges []string) []*Asset {
	res := make([]*Asset, 0, len(assets))
	for _, asset := range assets {
		exchangeFound := false
		for _, exchange := range exchanges {
			if asset.Exchange == exchange {
				exchangeFound = true
			}
		}
		if exchangeFound {
			res = append(res, asset)
		}
	}
	return res
}

// FilterAssetType removes assets that are not of one of the listed types
func FilterAssetType(assets []*Asset, assetTypes []string) []*Asset {
	res := make([]*Asset, 0, len(assets))
	for _, asset := range assets {
		include := false
		for _, assetType := range assetTypes {
			if asset.AssetType == assetType {
				include = true
			}
		}
		if include {
			res = append(res, asset)
		}
	}
	return res
}

func FilterAge(assets []*Asset, maxAge time.Duration) []*Asset {
	res := make([]*Asset, 0, len(assets))
	today := time.Now()
	for _, asset := range assets {
		endDate, err := time.Parse("2006-01-02", asset.EndDate)
		if err != nil {
			continue
		}

		age := today.Sub(endDate)
		if maxAge > age {
			res = append(res, asset)
		}
	}
	return res
}

func SaveToDatabase(quotes []*Eod, dsn string) error {
	log.Info().Msg("saving to database")
	conn, err := pgx.Connect(context.Background(), viper.GetString("DATABASE_URL"))
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("Could not connect to database")
	}
	defer conn.Close(context.Background())

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
			log.Error().Str("OriginalError", err.Error()).Str("Query", query).Msg("error saving EOD quote to database")
		}
	}

	return nil
}
