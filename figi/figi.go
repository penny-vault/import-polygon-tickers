package figi

import (
	"context"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/penny-vault/import-tickers/common"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

const (
	OPENFIGI_MAPPING_URL string = "https://api.openfigi.com/v3/mapping"
)

type MappingResponse struct {
	Data []*OpenFigiAsset `json:"data"`
}

type OpenFigiAsset struct {
	Figi                string `json:"figi"`
	SecurityType        string `json:"securityType"`
	MarketSector        string `json:"marketSector"`
	Ticker              string `json:"ticker"`
	Name                string `json:"name"`
	ExchangeCode        string `json:"exchCode"`
	ShareClassFIGI      string `json:"shareClassFIGI"`
	CompositeFIGI       string `json:"compositeFIGI"`
	SecurityType2       string `json:"securityType2"`
	SecurityDescription string `json:"securityDescription"`
}

type OpenFigiQuery struct {
	IdType       string `json:"idType"`
	IdValue      string `json:"idValue"`
	ExchangeCode string `json:"exchCode"`
}

func rateLimit() *rate.Limiter {
	dur := (time.Second * 6) / 25
	openFigiRate := rate.Every(dur)
	return rate.NewLimiter(openFigiRate, 10)
}

func mapFigis(query []*OpenFigiQuery) ([]*MappingResponse, error) {
	if len(query) > 100 {
		log.Error().Msg("programming error - too many assets in request")
	}

	apiKey := viper.GetString("openfigi.apikey")
	mappingResponse := make([]*MappingResponse, 0)
	client := resty.New()
	resp, err := client.R().
		SetHeader("X-OPENFIGI-APIKEY", apiKey).
		SetBody(query).
		SetResult(&mappingResponse).
		Post(OPENFIGI_MAPPING_URL)

	if err != nil {
		log.Error().Err(err).Msg("OpenFigi api called errored out")
		return []*MappingResponse{}, err
	}

	if resp.StatusCode() >= 400 {
		log.Error().Int("StatusCode", resp.StatusCode()).Str("Body", string(resp.Body())).Msg("openfigi api call returned invalid status code")
		return []*MappingResponse{}, err
	}

	return mappingResponse, nil
}

func Enrich(assets []*common.Asset) {
	rateLimiter := rateLimit()

	emptyFigis := make([]*common.Asset, 0, 100)
	for _, asset := range assets {
		if (asset.CompositeFigi == "" || asset.AssetType == common.UnknownAsset) && asset.DelistingDate == "" {
			emptyFigis = append(emptyFigis, asset)
		}
	}

	figiMap := LookupFigi(emptyFigis, rateLimiter)
	for _, asset := range emptyFigis {
		if assetFigi, ok := figiMap[asset.Ticker]; ok {
			asset.CompositeFigi = assetFigi.CompositeFIGI
			asset.ShareClassFigi = assetFigi.ShareClassFIGI

			if asset.AssetType == common.UnknownAsset {
				switch assetFigi.SecurityType2 {
				case "Partnership Shares":
					asset.AssetType = common.CommonStock
				case "Depositary Receipt":
					asset.AssetType = common.ADRC
				case "Common Stock":
					asset.AssetType = common.CommonStock
				case "Mutual Fund":
					switch assetFigi.SecurityType {
					case "ETP":
						asset.AssetType = common.ETF
					case "Open-End Fund":
						asset.AssetType = common.OpenEndFund
					case "Closed-End Fund":
						asset.AssetType = common.ClosedEndFund
					default:
						log.Warn().
							Str("SecurityType", assetFigi.SecurityType).
							Str("SecurityType2", assetFigi.SecurityType2).
							Str("Ticker", asset.Ticker).
							Str("CompositeFigi", assetFigi.CompositeFIGI).
							Msg("asset type is unknown and openfigi security type 2 is unknown")
					}
					asset.AssetType = common.OpenEndFund
				case "":
				default:
					log.Warn().
						Str("SecurityType", assetFigi.SecurityType).
						Str("SecurityType2", assetFigi.SecurityType2).
						Str("Ticker", asset.Ticker).
						Str("CompositeFigi", assetFigi.CompositeFIGI).
						Msg("asset type is unknown and openfigi security type is unknown")
				}
			}

			asset.LastUpdated = time.Now().Unix()
		}
	}
}

func LookupFigi(assets []*common.Asset, rateLimiter *rate.Limiter) map[string]*OpenFigiAsset {
	query := make([]*OpenFigiQuery, 0, 100)
	result := make(map[string]*OpenFigiAsset)
	bar := progressbar.Default(int64(len(assets)))

	for _, asset := range assets {
		bar.Add(1)
		query = append(query, &OpenFigiQuery{
			IdType:       "TICKER",
			IdValue:      asset.Ticker,
			ExchangeCode: "US",
		})

		if len(query) == 100 {
			rateLimiter.Wait(context.Background())
			mappingResponse, _ := mapFigis(query)
			for _, resp := range mappingResponse {
				for _, figiAsset := range resp.Data {
					result[figiAsset.Ticker] = figiAsset
				}
			}
			query = make([]*OpenFigiQuery, 0, 100)
		}
	}

	if len(query) > 0 {
		rateLimiter.Wait(context.Background())
		mappingResponse, _ := mapFigis(query)
		for _, resp := range mappingResponse {
			for _, figiAsset := range resp.Data {
				result[figiAsset.Ticker] = figiAsset
			}
		}
	}

	return result
}
