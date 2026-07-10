// Package conntest implements CON-140's "test connection" button: given a
// connection's type and (already-decrypted, by the caller) config and
// credential, attempts a real, bounded connectivity check. Runs entirely
// in the control plane process — it already carries the necessary client
// libraries, so no runtime round-trip is needed. Connection types without a
// concrete endpoint to probe (e.g. auth-only HTTP connections, or ones this
// increment didn't reach — see TODO.md) report success with an explanatory
// message rather than failing.
package conntest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/goburrow/modbus"
	"github.com/gopcua/opcua"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	_ "github.com/go-sql-driver/mysql"  // registers the "mysql" database/sql driver
	_ "github.com/jackc/pgx/v5/stdlib"  // registers the "pgx" database/sql driver
	_ "github.com/microsoft/go-mssqldb" // registers the "sqlserver" database/sql driver
	_ "modernc.org/sqlite"              // registers the "sqlite" database/sql driver

	"github.com/1uedev/DataPipe/engine/nodes/gem"
	"github.com/1uedev/DataPipe/engine/nodes/hsms"
	"github.com/1uedev/DataPipe/engine/nodes/kafkashared"
	"github.com/1uedev/DataPipe/engine/nodes/modbusshared"
	"github.com/1uedev/DataPipe/engine/nodes/mongoshared"
	"github.com/1uedev/DataPipe/engine/nodes/mqttshared"
	"github.com/1uedev/DataPipe/engine/nodes/opcuashared"
	"github.com/1uedev/DataPipe/engine/nodes/redisshared"
	"github.com/1uedev/DataPipe/engine/nodes/s3shared"
	"github.com/1uedev/DataPipe/engine/nodes/secsgemshared"
	"github.com/1uedev/DataPipe/engine/nodes/sqlshared"
)

// Timeout bounds how long a single test may take — this is invoked
// synchronously from an editor button click.
const Timeout = 10 * time.Second

// Result is CON-140's outcome: a browsable pass/fail with a human-readable
// reason.
type Result struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

// Test attempts a live connectivity check for connType, using config
// (non-secret) and credential (already decrypted — never logged or
// returned beyond this process boundary).
func Test(ctx context.Context, connType string, config, credential json.RawMessage) Result {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	switch connType {
	case "postgres", "mysql", "mssql", "sqlite":
		return testSQL(ctx, connType, config, credential)
	case "mqtt":
		return testMQTT(ctx, config, credential)
	case "mongodb":
		return testMongo(ctx, config, credential)
	case "redis":
		return testRedis(ctx, config, credential)
	case "kafka":
		return testKafka(ctx, config, credential)
	case "s3":
		return testS3(ctx, config, credential)
	case "modbus":
		return testModbus(ctx, config)
	case "opcua":
		return testOPCUA(ctx, config, credential)
	case "secsgem":
		return testSECSGEM(ctx, config)
	default:
		return Result{OK: true, Message: "no live test available for this connection type; config was accepted as-is"}
	}
}

func testSQL(ctx context.Context, connType string, config, credential json.RawMessage) Result {
	var cfg sqlshared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	var cred sqlshared.Credential
	if len(credential) > 0 {
		if err := json.Unmarshal(credential, &cred); err != nil {
			return Result{OK: false, Message: "invalid credential: " + err.Error()}
		}
	}
	driver, _, dsn, err := sqlshared.DSNFor(sqlshared.Dialect(connType), cfg, cred)
	if err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	return Result{OK: true, Message: "connected successfully"}
}

func testMQTT(ctx context.Context, config, credential json.RawMessage) Result {
	var cfg mqttshared.BrokerConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if cfg.BrokerURL == "" {
		return Result{OK: false, Message: "brokerUrl is required"}
	}
	var cred mqttshared.Credential
	if len(credential) > 0 {
		if err := json.Unmarshal(credential, &cred); err != nil {
			return Result{OK: false, Message: "invalid credential: " + err.Error()}
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.BrokerURL).
		SetClientID("datapipe-conntest-" + mqttshared.RandSuffix()).
		SetConnectTimeout(Timeout)
	if cred.Username != "" {
		opts.SetUsername(cred.Username)
		opts.SetPassword(cred.Password)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	waited := token.WaitTimeout(Timeout)
	defer client.Disconnect(100)

	if !waited {
		return Result{OK: false, Message: "connect timed out"}
	}
	if err := token.Error(); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	_ = ctx // the paho client manages its own timeout via SetConnectTimeout above
	return Result{OK: true, Message: "connected successfully"}
}

func testMongo(ctx context.Context, config, credential json.RawMessage) Result {
	var cfg mongoshared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if cfg.Host == "" || cfg.Database == "" {
		return Result{OK: false, Message: "host and database are required"}
	}
	port := cfg.Port
	if port == 0 {
		port = 27017
	}
	var cred mongoshared.Credential
	if len(credential) > 0 {
		if err := json.Unmarshal(credential, &cred); err != nil {
			return Result{OK: false, Message: "invalid credential: " + err.Error()}
		}
	}
	uri := fmt.Sprintf("mongodb://%s:%d", cfg.Host, port)
	opts := options.Client().ApplyURI(uri)
	if cred.Username != "" {
		opts.SetAuth(options.Credential{Username: cred.Username, Password: cred.Password, AuthSource: cfg.AuthSource})
	}
	client, err := mongodriver.Connect(opts)
	if err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	if err := client.Ping(ctx, nil); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	return Result{OK: true, Message: "connected successfully"}
}

func testRedis(ctx context.Context, config, credential json.RawMessage) Result {
	var cfg redisshared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if cfg.Host == "" {
		return Result{OK: false, Message: "host is required"}
	}
	port := cfg.Port
	if port == 0 {
		port = 6379
	}
	var cred redisshared.Credential
	if len(credential) > 0 {
		if err := json.Unmarshal(credential, &cred); err != nil {
			return Result{OK: false, Message: "invalid credential: " + err.Error()}
		}
	}
	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.Host, port), Username: cred.Username, Password: cred.Password, DB: cfg.DB,
	})
	defer func() { _ = client.Close() }()

	if err := client.Ping(ctx).Err(); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	return Result{OK: true, Message: "connected successfully"}
}

func testKafka(ctx context.Context, config, _ json.RawMessage) Result {
	var cfg kafkashared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if len(cfg.Brokers) == 0 {
		return Result{OK: false, Message: "at least one broker is required"}
	}
	conn, err := (&kafka.Dialer{Timeout: Timeout}).DialContext(ctx, "tcp", cfg.Brokers[0])
	if err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Brokers(); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	return Result{OK: true, Message: "connected successfully"}
}

func testS3(ctx context.Context, config, credential json.RawMessage) Result {
	var cfg s3shared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if cfg.Bucket == "" {
		return Result{OK: false, Message: "bucket is required"}
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	var cred s3shared.Credential
	if len(credential) > 0 {
		if err := json.Unmarshal(credential, &cred); err != nil {
			return Result{OK: false, Message: "invalid credential: " + err.Error()}
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
		return Result{OK: false, Message: err.Error()}
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = &cfg.Endpoint
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &cfg.Bucket}); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	return Result{OK: true, Message: "connected successfully"}
}

func testModbus(ctx context.Context, config json.RawMessage) Result {
	var cfg modbusshared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if cfg.Mode != "tcp" {
		return Result{OK: true, Message: "no live test available for serial (RTU) connections; config was accepted as-is"}
	}
	if cfg.TCP.Host == "" || cfg.TCP.Port == 0 {
		return Result{OK: false, Message: "tcp.host and tcp.port are required"}
	}
	h := modbus.NewTCPClientHandler(fmt.Sprintf("%s:%d", cfg.TCP.Host, cfg.TCP.Port))
	h.Timeout = Timeout
	if err := h.Connect(); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	defer func() { _ = h.Close() }()
	return Result{OK: true, Message: "connected successfully"}
}

func testOPCUA(ctx context.Context, config, credential json.RawMessage) Result {
	var cfg opcuashared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if cfg.Endpoint == "" {
		return Result{OK: false, Message: "endpoint is required"}
	}
	var cred opcuashared.Credential
	if len(credential) > 0 {
		if err := json.Unmarshal(credential, &cred); err != nil {
			return Result{OK: false, Message: "invalid credential: " + err.Error()}
		}
	}

	opts := []opcua.Option{opcua.SecurityPolicy(orDefault(cfg.SecurityPolicy, "None")), opcua.SecurityModeString(orDefault(cfg.SecurityMode, "None"))}
	if cfg.Auth == "username" {
		opts = append(opts, opcua.AuthUsername(cred.Username, cred.Password))
	} else {
		opts = append(opts, opcua.AuthAnonymous())
	}
	client, err := opcua.NewClient(cfg.Endpoint, opts...)
	if err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	if err := client.Connect(ctx); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	defer func() { _ = client.Close(ctx) }()
	return Result{OK: true, Message: "connected successfully"}
}

func testSECSGEM(ctx context.Context, config json.RawMessage) Result {
	var cfg secsgemshared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if cfg.Port == 0 {
		return Result{OK: false, Message: "port is required"}
	}
	if cfg.Mode != "active" {
		return Result{OK: true, Message: "no live test available for mode \"passive\" connections (the equipment dials in, not the control plane); config was accepted as-is"}
	}
	if cfg.Host == "" {
		return Result{OK: false, Message: "host is required for mode \"active\""}
	}

	timers := cfg.HSMSTimers()
	timers.T6 = Timeout
	conn, err := hsms.Dial(ctx, cfg.Addr(), uint16(cfg.SessionID), timers)
	if err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	defer func() { _ = conn.Separate() }()

	mdln, softrev := cfg.Identity()
	host := gem.NewHost(conn, mdln, softrev)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = host.Run(runCtx) }()

	equipMDLN, equipSoftrev, err := host.AreYouThere(ctx)
	if err != nil {
		return Result{OK: false, Message: "HSMS Select succeeded but Are-You-There (S1F1) failed: " + err.Error()}
	}
	return Result{OK: true, Message: fmt.Sprintf("connected, selected, and equipment responded (model %q, software revision %q)", equipMDLN, equipSoftrev)}
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
