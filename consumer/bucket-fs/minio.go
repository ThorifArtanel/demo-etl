package bucketfs

import (
	"context"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog/log"
)

func StoreToBucket(objectName, fileName string) error {
	minioCtx := context.Background()
	endpoint := "host.docker.internal:9000"
	accessKeyID := "R3CPFN8PWLCYN06J6OJ7"
	secretAccessKey := "8KVcXpaNLuiiC7pVD2E7wevImSUj4iI1u5vaaC1l"
	bucketName := "demo-etl-generated"
	useSSL := false
	contentType := "application/octet-stream"

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	_, err = minioClient.FPutObject(minioCtx, bucketName, objectName, fileName, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return err
	}

	log.Info().Msgf("Successfully uploaded %s", objectName)

	return nil
}
