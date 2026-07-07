module github.com/1uedev/DataPipe/engine

go 1.26.4

require (
	github.com/1uedev/DataPipe/proto/gen/go v0.0.0
	github.com/google/uuid v1.6.0
	github.com/oklog/ulid/v2 v2.1.1
	google.golang.org/grpc v1.82.0
)

require (
	github.com/dlclark/regexp2/v2 v2.2.1 // indirect
	github.com/dop251/goja v0.0.0-20260701091749-b07b74453ea9 // indirect
	github.com/eclipse/paho.mqtt.golang v1.5.1 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/santhosh-tekuri/jsonschema/v5 v5.3.1 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/1uedev/DataPipe/proto/gen/go => ../proto/gen/go
