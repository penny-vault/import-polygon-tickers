package backblaze

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kothar/go-backblaze"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func UploadToBackBlaze(fn, bucketName, dirname string) error {
	b2, err := backblaze.NewB2(backblaze.Credentials{
		KeyID:          viper.GetString("backblaze_application_id"),
		ApplicationKey: viper.GetString("backblaze_application_key"),
	})
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Str("BucketName", bucketName).Msg("authorize backblaze failed")
		return err
	}

	bucket, err := b2.Bucket(bucketName)
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Str("BucketName", bucketName).Msg("lookup bucket failed")
		return err
	}
	if bucket == nil {
		log.Error().Str("BucketName", bucketName).Msg("bucket does not exist")
		return errors.New("bucket not found")
	}

	reader, _ := os.Open(fn)
	defer reader.Close()

	outName := fmt.Sprintf("%s/%s", dirname, filepath.Base(fn))
	metadata := make(map[string]string)

	file, err := bucket.UploadFile(outName, metadata, reader)
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Str("FileName", outName).Str("BucketName", bucketName).Msg("save file to backblaze failed")
		return err
	}

	log.Info().Str("FileName", file.Name).Int64("Size", file.ContentLength).Str("ID", file.ID).Msg("uploaded file to backblaze")
	return nil
}
