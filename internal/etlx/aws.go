package etlxlib

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/realdatadriven/etlx/internal/env"
)

// awsConfig returns an AWS config for SDK v2
func (etlx *ETLX) awsConfig(ctx context.Context, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN string) (aws.Config, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(AWS_REGION),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			AWS_ACCESS_KEY_ID,
			AWS_SECRET_ACCESS_KEY,
			AWS_SESSION_TOKEN,
		)),
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return cfg, fmt.Errorf("failed to load AWS config: %v", err)
	}
	return cfg, nil
}

// fileExistsInS3 checks if a file exists in the given S3 bucket
func (etlx *ETLX) FileExistsInS3(ctx context.Context, client *s3.Client, bucket, key string) bool {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err == nil
}

func (etlx *ETLX) S3(mode string, params map[string]any) (string, error) {
	// Create AWS session
	AWS_ACCESS_KEY_ID, ok := params["AWS_ACCESS_KEY_ID"].(string)
	if !ok {
		AWS_ACCESS_KEY_ID = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	AWS_SECRET_ACCESS_KEY, ok := params["AWS_SECRET_ACCESS_KEY"].(string)
	if !ok {
		AWS_SECRET_ACCESS_KEY = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	AWS_SESSION_TOKEN, ok := params["AWS_SESSION_TOKEN"].(string)
	if !ok {
		AWS_SESSION_TOKEN = os.Getenv("AWS_SESSION_TOKEN")
	}
	AWS_REGION, ok := params["AWS_REGION"].(string)
	if !ok {
		AWS_REGION = os.Getenv("AWS_REGION")
	}
	AWS_ENDPOINT, ok := params["AWS_ENDPOINT"].(string)
	if !ok {
		AWS_ENDPOINT = os.Getenv("AWS_ENDPOINT")
	}
	S3_FORCE_PATH_STYLE, ok := params["S3_FORCE_PATH_STYLE"].(bool)
	if !ok {
		S3_FORCE_PATH_STYLE = env.GetBool("S3_FORCE_PATH_STYLE", false)
	}
	/*S3_SKIP_SSL_VERIFY, ok := params["S3_SKIP_SSL_VERIFY"].(bool)
	if !ok {
		S3_SKIP_SSL_VERIFY = env.GetBool("S3_SKIP_SSL_VERIFY", false)
	}
	S3_DISABLE_SSL, ok := params["S3_DISABLE_SSL"].(bool)
	if !ok {
		S3_DISABLE_SSL = env.GetBool("S3_DISABLE_SSL", false)
	}*/
	ctx := context.Background()
	cfg, err := etlx.awsConfig(ctx, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint := AWS_ENDPOINT; endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = S3_FORCE_PATH_STYLE
	})
	// Define the S3 bucket and key
	bucket := params["bucket"].(string)
	originalKey := params["key"].(string)
	//ext := filepath.Ext(originalKey)
	//baseName := originalKey[:len(originalKey)-len(ext)]
	// Check if the file already exists and modify the file name if necessary
	key := originalKey
	for i := 1; etlx.FileExistsInS3(ctx, client, bucket, key); i++ {
		//key = fmt.Sprintf("%s_%d%s", baseName, i, ext)
	}
	if mode == "upload" {
		source, _ := params["source"].(string)
		file, err := os.Open(source)
		if err != nil {
			return "", fmt.Errorf("opening source file failed: %w", err)
		}
		defer file.Close()
		// Read file into a buffer to allow seeking
		var buffer bytes.Buffer
		if _, err := io.Copy(&buffer, file); err != nil {
			return "", fmt.Errorf("failed to read file into buffer: %v", err)
		}
		// Upload file to S3
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(buffer.Bytes()),
			//ACL:    types.ObjectCannedACLPublicRead, // Optional: Set ACL for public access if needed
		})
		if err != nil {
			return "", fmt.Errorf("failed to upload to S3: %v", err)
		}
		return key, nil
	} else if mode == "download" {
		target, _ := params["target"].(string)
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return "", fmt.Errorf("failed to get file from S3 %v", err)
		}
		defer resp.Body.Close()
		outFile, err := os.Create(target)
		if err != nil {
			return "", fmt.Errorf("creating target file failed: %w", err)
		}
		defer outFile.Close()
		_, err = io.Copy(outFile, resp.Body)
		if err != nil {
			return "", fmt.Errorf("writing to target file failed: %w", err)
		}
		return key, nil
	} else {
		return "", fmt.Errorf("%s not suported", mode)
	}
}
