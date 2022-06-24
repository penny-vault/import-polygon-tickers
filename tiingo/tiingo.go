package tiingo

import (
	"archive/zip"
	"bytes"
	"io/ioutil"
	"regexp"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/gocarina/gocsv"
	"github.com/penny-vault/import-tickers/common"
	"github.com/rs/zerolog/log"
)

type TiingoAsset struct {
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

// ignoreTicker interprets the structure of the ticker to identify
// the share type (Warrant, Unit, Preferred Share, etc.) and filters
// out unsupported stock types
func ignoreTicker(ticker string) bool {
	ignore := strings.HasPrefix(ticker, "ATEST")
	ignore = ignore || strings.HasPrefix(ticker, "NTEST")
	ignore = ignore || strings.HasPrefix(ticker, "PTEST")
	ignore = ignore || strings.Contains(ticker, " ")
	matcher := regexp.MustCompile(`^[A-Za-z0-9]+-W?P?U?.*$`)
	ignore = ignore || matcher.Match([]byte(ticker))
	matcher = regexp.MustCompile(`^[A-Za-z0-9]{4}[WPU]{1}.*$`)
	ignore = ignore || matcher.Match([]byte(ticker))
	return ignore
}

// FetchAssets retrieves a list of supported tickers from Tiingo
func FetchAssets() []*common.Asset {
	tickerUrl := "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
	client := resty.New()
	assets := []*TiingoAsset{}

	resp, err := client.
		R().
		Get(tickerUrl)
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to download tickers")
		return []*common.Asset{}
	}
	if resp.StatusCode() >= 400 {
		log.Error().Int("StatusCode", resp.StatusCode()).Str("Url", tickerUrl).Bytes("Body", resp.Body()).Msg("error when requesting eod quote")
		return []*common.Asset{}
	}

	// unzip downloaded data
	body := resp.Body()
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("could not read response body when downloading tickers")
		return []*common.Asset{}
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to read tickers zip file")
		return []*common.Asset{}
	}

	// Read all the files from zip archive
	var tickerCsvBytes []byte
	if len(zipReader.File) == 0 {
		log.Error().Msg("no files contained in received zip file")
		return []*common.Asset{}
	}

	zipFile := zipReader.File[0]
	tickerCsvBytes, err = readZipFile(zipFile)
	if err != nil {
		log.Error().Err(err).Msg("failed to read ticker csv from zip")
		return []*common.Asset{}
	}

	if err := gocsv.UnmarshalBytes(tickerCsvBytes, &assets); err != nil {
		log.Error().Err(err).Msg("failed to unmarshal csv")
		return []*common.Asset{}
	}

	validExchanges := []string{"AMEX", "BATS", "NASDAQ", "NMFQS", "NYSE", "NYSE ARCA", "NYSE MKT"}
	commonAssets := make([]*common.Asset, 0, 25000)
	for _, asset := range assets {
		// remove assets on invalid exchanges
		keep := false
		for _, exchange := range validExchanges {
			if asset.Exchange == exchange {
				keep = true
			}
		}
		if !keep {
			continue
		}

		// If both the start date and end date are not set skip it
		if asset.StartDate == "" && asset.EndDate == "" {
			continue
		}

		// filter out tickers we should ignore
		if ignoreTicker(asset.Ticker) {
			continue
		}

		asset.Ticker = strings.ReplaceAll(asset.Ticker, "-", "/")
		myAsset := &common.Asset{
			Ticker:          asset.Ticker,
			ListingDate:     asset.StartDate,
			DelistingDate:   asset.EndDate,
			PrimaryExchange: asset.Exchange,
			Source:          "api.tiingo.com",
		}

		switch asset.AssetType {
		case "Stock":
			myAsset.AssetType = common.CommonStock
		case "ETF":
			myAsset.AssetType = common.ETF
		case "Mutual Fund":
			myAsset.AssetType = common.MutualFund
		}

		if asset.EndDate != "" {
			endDate, err := time.Parse("2006-01-02", asset.EndDate)
			if err != nil {
				log.Warn().Str("EndDate", asset.EndDate).Err(err).Msg("could not parse end date")
			}
			now := time.Now()
			age := now.Sub(endDate)
			if age < (time.Hour * 24 * 7) {
				myAsset.DelistingDate = ""
			}
		}

		if myAsset.DelistingDate == "" {
			commonAssets = append(commonAssets, myAsset)
		}
	}

	return commonAssets
}
