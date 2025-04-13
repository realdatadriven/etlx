package etlxlib

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/realdatadriven/etlx/internal/env"
)

func (etlx *ETLX) FileExistsInS3(svc *s3.S3, bucket, key string) bool {
	_, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	// If no error, the file exists
	return err == nil
}

func (etlx *ETLX) AWSSession(AWS_ACCESS_KEY_ID string, AWS_SECRET_ACCESS_KEY string, AWS_SESSION_TOKEN string, AWS_REGION string, AWS_ENDPOINT string, S3_FORCE_PATH_STYLE bool, S3_SKIP_SSL_VERIFY bool, S3_DISABLE_SSL bool) (*session.Session, error) {
	awsConfig := &aws.Config{
		Region: aws.String(AWS_REGION),
		Credentials: credentials.NewStaticCredentials(
			AWS_ACCESS_KEY_ID,
			AWS_SECRET_ACCESS_KEY,
			AWS_SESSION_TOKEN, // Optional
		),
		Endpoint:         aws.String(AWS_ENDPOINT),      // Optional custom endpoint,
		S3ForcePathStyle: aws.Bool(S3_FORCE_PATH_STYLE), // Force path-style URLs (necessary for MinIO)
		DisableSSL:       aws.Bool(S3_DISABLE_SSL),      // MinIO often runs without SSL locally
	}
	// Create a custom HTTP client that skips SSL certificate verification
	if S3_SKIP_SSL_VERIFY && S3_FORCE_PATH_STYLE {
		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // Disable SSL verification
		awsConfig = &aws.Config{
			Region: aws.String(AWS_REGION),
			Credentials: credentials.NewStaticCredentials(
				AWS_ACCESS_KEY_ID,
				AWS_SECRET_ACCESS_KEY,
				AWS_SESSION_TOKEN, // Optional
			),
			Endpoint:         aws.String(AWS_ENDPOINT),                 // Optional custom endpoint,
			S3ForcePathStyle: aws.Bool(S3_FORCE_PATH_STYLE),            // Force path-style URLs (necessary for MinIO)
			DisableSSL:       aws.Bool(S3_DISABLE_SSL),                 // MinIO often runs without SSL locally
			HTTPClient:       &http.Client{Transport: customTransport}, // Use the custom transport with TLS config
		}
	}
	// Create AWS session
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}
	return sess, nil
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
	S3_SKIP_SSL_VERIFY, ok := params["S3_SKIP_SSL_VERIFY"].(bool)
	if !ok {
		S3_SKIP_SSL_VERIFY = env.GetBool("S3_SKIP_SSL_VERIFY", false)
	}
	S3_DISABLE_SSL, ok := params["S3_DISABLE_SSL"].(bool)
	if !ok {
		S3_DISABLE_SSL = env.GetBool("S3_DISABLE_SSL", false)
	}
	sess, err := etlx.AWSSession(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION, AWS_ENDPOINT, S3_FORCE_PATH_STYLE, S3_SKIP_SSL_VERIFY, S3_DISABLE_SSL)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS session: %v", err)
	}
	// Create S3 service client
	svc := s3.New(sess)
	// Define the S3 bucket and key
	bucket := params["bucket"].(string)
	originalKey := params["key"].(string)
	//ext := filepath.Ext(originalKey)
	//baseName := originalKey[:len(originalKey)-len(ext)]
	// Check if the file already exists and modify the file name if necessary
	key := originalKey
	for i := 1; etlx.FileExistsInS3(svc, bucket, key); i++ {
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
		// Convert buffer into a ReadSeeker
		fileReader := bytes.NewReader(buffer.Bytes())
		// Upload file to S3
		_, err = svc.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   fileReader,
			//ACL:    aws.String("public-read"), // Optional: Set ACL for public access if needed
		})
		if err != nil {
			return "", fmt.Errorf("failed to upload to S3: %v", err)
		}
		return key, nil
	} else if mode == "download" {
		target, _ := params["target"].(string)
		resp, err := svc.GetObject(&s3.GetObjectInput{
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
