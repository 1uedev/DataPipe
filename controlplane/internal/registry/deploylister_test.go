package registry

import (
	"context"
	"testing"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
)

type fakeDeployedFlowsLister struct{ flows []DeployedFlowInfo }

func (f *fakeDeployedFlowsLister) ListDeployedFlows(ctx context.Context) ([]DeployedFlowInfo, error) {
	return f.flows, nil
}

// TestERR150_DeployStreamOpenPushesEveryCurrentlyDeployedFlow proves a
// (re)connecting runtime — including one recovering from a crash — gets
// every currently-deployed flow re-pushed automatically, without waiting
// for the next REST deploy ("runtime restart restores all deployed flows
// and durable state automatically").
func TestERR150_DeployStreamOpenPushesEveryCurrentlyDeployedFlow(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	svc.SetDeployedFlowsLister(&fakeDeployedFlowsLister{flows: []DeployedFlowInfo{
		{FlowID: "flow-1", Version: 3, ContentJSON: `{"id":"flow-1"}`, DefaultErrorFlow: "flow_err"},
		{FlowID: "flow-2", Version: 1, ContentJSON: `{"id":"flow-2"}`},
	}})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1", Version: "0.0.1"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	stream, err := client.DeployStream(ctx, &runtimev1.DeployStreamRequest{RuntimeId: "rt-1", SessionToken: resp.GetSessionToken()})
	if err != nil {
		t.Fatalf("DeployStream: %v", err)
	}

	seen := map[string]*runtimev1.DeployStreamResponse{}
	for len(seen) < 2 {
		cmd, err := stream.Recv()
		if err != nil {
			t.Fatalf("stream.Recv: %v", err)
		}
		seen[cmd.GetFlowId()] = cmd
	}

	flow1 := seen["flow-1"]
	if flow1 == nil || flow1.GetVersion() != 3 || flow1.GetFlowJson() != `{"id":"flow-1"}` || flow1.GetDefaultErrorFlow() != "flow_err" {
		t.Fatalf("flow-1 = %+v, want version 3 with default_error_flow=flow_err", flow1)
	}
	flow2 := seen["flow-2"]
	if flow2 == nil || flow2.GetVersion() != 1 {
		t.Fatalf("flow-2 = %+v, want version 1", flow2)
	}
}

func TestERR150_DeployStreamOpenWithNoListerConfiguredStillWorks(t *testing.T) {
	client, _, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1", Version: "0.0.1"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := client.DeployStream(ctx, &runtimev1.DeployStreamRequest{RuntimeId: "rt-1", SessionToken: resp.GetSessionToken()}); err != nil {
		t.Fatalf("DeployStream: %v", err)
	}
	// No assertion beyond "doesn't hang or error" — SetDeployedFlowsLister
	// was never called, exercising the nil-lister path.
}
