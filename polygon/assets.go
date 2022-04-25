package polygon

import (
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type PolygonAssetTypesResponse struct {
	Results   []*AssetTypes
	Status    string
	RequestId string
	Count     int
}

type AssetTypes struct {
	Code        string
	Description string
	AssetClass  string
	Locale      string
}

func ListSupportedAssetTypes() []*AssetTypes {
	polygonToken := viper.GetString("polygon_token")
	url := fmt.Sprintf("https://api.polygon.io/v3/reference/tickers/types?asset_class=stocks&apiKey=%s", polygonToken)
	client := resty.New()
	assetTypes := []*AssetTypes{}

	resp, err := client.
		R().
		Get(url)

	if err != nil {
		log.Error().Str("Url", url).Str("OriginalError", err.Error()).Msg("error when fetching list of supported asset types")
		return assetTypes
	}

	if resp.StatusCode() >= 400 {
		log.Error().Str("Url", url).Int("StatusCode", resp.StatusCode()).Msg("error code received from server when fetching supported asset types")
	}

	body := resp.Body()
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("could not read response body when fetching asset types")
		return assetTypes
	}

	assetTypesResponse := PolygonAssetTypesResponse{}
	if err := json.Unmarshal(body, &assetTypesResponse); err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("could not unmarshal response body when fetching asset types")
		return assetTypes
	}

	if assetTypesResponse.Status != "OK" {
		log.Error().Str("PolygonStatus", assetTypesResponse.Status).Str("OriginalError", err.Error()).Msg("polygon status code not OK")
		return assetTypes
	}

	return assetTypesResponse.Results
}
