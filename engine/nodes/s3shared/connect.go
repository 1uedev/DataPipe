// Package s3shared is the connection-resolution code shared by "s3-source"
// and "s3-sink" (CON-400's S3-compatible object storage clause / Increment
// 10 MVP catalog "S3 files"). A custom endpoint + path-style addressing is
// supported so this also covers S3-compatible object stores (MinIO, etc.),
// not just AWS.
package s3shared

import (
	"context"
	"encoding/json"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/1uedev/DataPipe/engine/flow"
)

// Config is an "s3" connection's non-secret config.
type Config struct {
	Region         string `json:"region,omitempty"`
	Bucket         string `json:"bucket"`
	Endpoint       string `json:"endpoint,omitempty"`       // non-empty for S3-compatible stores (MinIO, etc.)
	ForcePathStyle bool   `json:"forcePathStyle,omitempty"` // required by most non-AWS S3-compatible stores
}

// Credential is an "s3" connection's credential shape.
type Credential struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

// Resolved is a resolved s3 connection: a ready client plus the target
// bucket.
type Resolved struct {
	Client *s3.Client
	Bucket string
}

// Connect resolves the calling node's connection into a Resolved client.
func Connect(ctx context.Context) (Resolved, error) {
	info, err := flow.ResolveConnection(ctx)
	if err != nil {
		return Resolved{}, fmt.Errorf("s3shared: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(info.Config, &cfg); err != nil {
		return Resolved{}, fmt.Errorf("s3shared: parsing connection config: %w", err)
	}
	if cfg.Bucket == "" {
		return Resolved{}, fmt.Errorf("s3shared: connection config requires bucket")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	var cred Credential
	if len(info.CredentialJSON) > 0 {
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return Resolved{}, fmt.Errorf("s3shared: parsing credential: %w", err)
		}
	}

	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(region))
	if cred.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cred.AccessKeyID, cred.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return Resolved{}, fmt.Errorf("s3shared: loading AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = &cfg.Endpoint
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	return Resolved{Client: client, Bucket: cfg.Bucket}, nil
}
