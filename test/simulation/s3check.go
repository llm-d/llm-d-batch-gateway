//go:build simulation

/*
Copyright 2026 The llm-d Authors

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

package simulation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3Credentials returns the harness bucket credentials: per-run generated
// for compose, the dev-deploy chart's for kind.
func s3Credentials() (accessKey, secretKey string) {
	if backendName() == "kind" {
		return "minioadmin", "minioadmin"
	}
	return simS3AccessKeyID, simCreds.s3SecretKey
}

// The bucket and its host-reachable endpoint differ per backend: compose
// exposes MinIO on 19000 with bucket "batch-gateway"; dev-deploy uses
// NodePort 30009 and bucket "llm-d-batch-gateway".
func simBucket() string {
	if backendName() == "kind" {
		return "llm-d-batch-gateway"
	}
	return "batch-gateway"
}

func minioEndpoint() string {
	if backendName() == "kind" {
		return "http://127.0.0.1:9002"
	}
	return "http://127.0.0.1:19000"
}

// listBucketKeys returns every object key in the harness bucket.
func listBucketKeys(ctx context.Context) ([]string, error) {
	s3AccessKey, s3SecretKey := s3Credentials()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(s3AccessKey, s3SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint())
		o.UsePathStyle = true
	})

	var keys []string
	var token *string
	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(simBucket()),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range out.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			return keys, nil
		}
		token = out.NextContinuationToken
	}
}

// orphanedBlobs returns bucket keys whose file ID has no file record in the
// API. Blob keys have the form <tenant-folder>/<fileID>.<ext>.
func orphanedBlobs(client *apiClient) ([]string, error) {
	files, err := client.listFiles()
	if err != nil {
		return nil, err
	}
	known := make(map[string]bool, len(files))
	for _, f := range files {
		known[f.ID] = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	keys, err := listBucketKeys(ctx)
	if err != nil {
		return nil, err
	}

	var orphans []string
	for _, key := range keys {
		base := key
		if i := strings.LastIndex(base, "/"); i >= 0 {
			base = base[i+1:]
		}
		fileID := base
		if i := strings.LastIndex(fileID, "."); i >= 0 {
			fileID = fileID[:i]
		}
		if !known[fileID] {
			orphans = append(orphans, key)
		}
	}
	return orphans, nil
}

// clearBucket deletes every object in the harness bucket.
func clearBucket(ctx context.Context) error {
	s3AccessKey, s3SecretKey := s3Credentials()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(s3AccessKey, s3SecretKey, "")),
	)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint())
		o.UsePathStyle = true
	})
	keys, err := listBucketKeys(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(simBucket()),
			Key:    aws.String(key),
		}); err != nil {
			return fmt.Errorf("delete %s: %w", key, err)
		}
	}
	return nil
}
