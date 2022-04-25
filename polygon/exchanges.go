package polygon

import (
	"github.com/go-resty/resty/v2"
	"github.com/gocarina/gocsv"
	"github.com/rs/zerolog/log"
)

type ExchangeCode struct {
	Country        string `csv:"COUNTRY"`
	ISOCountryCode string `csv:"ISO COUNTRY CODE (ISO 3166)"`
	Mic            string `csv:"MIC"`
	OperatingMic   string `csv:"OPERATING MIC"`
	OorS           string `csv:"O/S"`
	Name           string `csv:"NAME-INSTITUTION DESCRIPTION"`
	Acronym        string `csv:"ACRONYM"`
	City           string `csv:"CITY"`
	Website        string `csv:"WEBSITE"`
	StatusDate     string `csv:"STATUS DATE"`
	Status         string `csv:"STATUS"`
	CreationDate   string `csv:"CREATION DATE"`
	Comments       string `csv:"COMMENTS"`
}

func ListExchangeCodes() []*ExchangeCode {
	url := "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"
	client := resty.New()
	exchangeCodes := []*ExchangeCode{}

	resp, err := client.
		R().
		Get(url)

	if err != nil {
		log.Error().Str("Url", url).Str("OriginalError", err.Error()).Msg("error when fetching list of exchange codes")
		return exchangeCodes
	}

	if resp.StatusCode() >= 400 {
		log.Error().Str("Url", url).Int("StatusCode", resp.StatusCode()).Msg("error code received from server when fetching exchange codes")
	}

	body := resp.Body()
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("could not read response body when fetching exchange codes")
		return exchangeCodes
	}

	if err := gocsv.UnmarshalBytes(body, &exchangeCodes); err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to unmarshal csv")
		return exchangeCodes
	}

	return exchangeCodes
}
