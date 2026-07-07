package registry

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
)

// fakeDeviceStore is an in-memory DeviceStore test double: tokens map to a
// group name, and enrolling a runtime records which token it used so a
// mismatched re-registration can be rejected the same way api.Store does.
type fakeDeviceStore struct {
	mu             sync.Mutex
	validTokens    map[string]string // token -> group
	enrolledDevice map[string]string // runtimeID -> token used
	groups         map[string]string // runtimeID -> group
}

func newFakeDeviceStore() *fakeDeviceStore {
	return &fakeDeviceStore{
		validTokens:    map[string]string{},
		enrolledDevice: map[string]string{},
		groups:         map[string]string{},
	}
}

func (f *fakeDeviceStore) Authenticate(ctx context.Context, runtimeID, kind, enrollmentToken string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if enrollmentToken == "" {
		if _, alreadyEnrolled := f.enrolledDevice[runtimeID]; alreadyEnrolled {
			return fmt.Errorf("runtime %q requires its enrollment token", runtimeID)
		}
		return nil
	}
	group, ok := f.validTokens[enrollmentToken]
	if !ok {
		return fmt.Errorf("invalid token")
	}
	if used, exists := f.enrolledDevice[runtimeID]; exists && used != enrollmentToken {
		return fmt.Errorf("runtime %q already enrolled with a different token", runtimeID)
	}
	f.enrolledDevice[runtimeID] = enrollmentToken
	f.groups[runtimeID] = group
	return nil
}

func (f *fakeDeviceStore) GroupOf(ctx context.Context, runtimeID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.groups[runtimeID], nil
}

func (f *fakeDeviceStore) DeviceInfo(ctx context.Context, runtimeID string) (DeviceInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, enrolled := f.enrolledDevice[runtimeID]
	return DeviceInfo{GroupName: f.groups[runtimeID], Enrolled: enrolled}, nil
}

func TestEDGE120_RegisterRejectsInvalidEnrollmentToken(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	svc.SetDeviceStore(newFakeDeviceStore())

	resp, err := client.Register(context.Background(), &runtimev1.RegisterRequest{RuntimeId: "edge-1", EnrollmentToken: "bogus"})
	if err == nil {
		t.Fatal("expected Register to reject an invalid enrollment token")
	}
	if resp.GetAccepted() {
		t.Fatal("expected Accepted=false on rejection")
	}
}

func TestEDGE120_RegisterAcceptsValidEnrollmentTokenAndEnforcesItOnRenewal(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	store := newFakeDeviceStore()
	store.validTokens["good-token"] = "edge-fab2"
	svc.SetDeviceStore(store)

	ctx := context.Background()
	if _, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "edge-1", EnrollmentToken: "good-token"}); err != nil {
		t.Fatalf("Register with a valid token: %v", err)
	}
	if _, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "edge-1"}); err == nil {
		t.Fatal("expected re-registration without the token to be rejected once enrolled")
	}
	if _, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "edge-1", EnrollmentToken: "good-token"}); err != nil {
		t.Fatalf("re-registration with the same token should succeed: %v", err)
	}
}

func TestEDGE120_HeartbeatHealthAppearsInListRuntimes(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1", Version: "1.0.0"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, err = client.Heartbeat(ctx, &runtimev1.HeartbeatRequest{
		RuntimeId: "rt-1", SessionToken: resp.GetSessionToken(),
		CpuPercent: 12.5, MemoryBytes: 1024,
		FlowStatuses: []*runtimev1.FlowStatus{{FlowId: "flow-a", Status: "running"}},
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	snaps := svc.ListRuntimes(ctx)
	if len(snaps) != 1 {
		t.Fatalf("ListRuntimes = %d entries, want 1", len(snaps))
	}
	s := snaps[0]
	if s.FlowCount != 1 {
		t.Errorf("FlowCount = %d, want 1", s.FlowCount)
	}
	// Health is only reported while a DeployStream is open (Online); this
	// runtime never opened one, so it should read as offline with no
	// CPU/memory snapshot even though Heartbeat carried some — matches
	// "online" meaning "actually able to receive work right now".
	if s.Online {
		t.Errorf("expected Online=false (no DeployStream opened), got true")
	}
}

func TestEDGE120_ListRuntimesMergesDeviceStoreMetadata(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	store := newFakeDeviceStore()
	store.validTokens["tok"] = "edge-fab2"
	svc.SetDeviceStore(store)
	ctx := context.Background()

	if _, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "edge-1", EnrollmentToken: "tok"}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	snaps := svc.ListRuntimes(ctx)
	if len(snaps) != 1 || !snaps[0].Enrolled || snaps[0].Group != "edge-fab2" {
		t.Fatalf("snapshot = %+v, want enrolled in edge-fab2", snaps)
	}
}

func TestEDGE120_DeployFlowOnlyReachesRuntimesInTheTargetGroup(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	store := newFakeDeviceStore()
	store.validTokens["fab2-token"] = "edge-fab2"
	svc.SetDeviceStore(store)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// rt-edge is in edge-fab2; rt-server is ungrouped.
	edgeResp, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-edge", EnrollmentToken: "fab2-token"})
	if err != nil {
		t.Fatalf("Register rt-edge: %v", err)
	}
	serverResp, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-server"})
	if err != nil {
		t.Fatalf("Register rt-server: %v", err)
	}

	edgeStream, err := client.DeployStream(ctx, &runtimev1.DeployStreamRequest{RuntimeId: "rt-edge", SessionToken: edgeResp.GetSessionToken()})
	if err != nil {
		t.Fatalf("DeployStream rt-edge: %v", err)
	}
	serverStream, err := client.DeployStream(ctx, &runtimev1.DeployStreamRequest{RuntimeId: "rt-server", SessionToken: serverResp.GetSessionToken()})
	if err != nil {
		t.Fatalf("DeployStream rt-server: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // let both streams register server-side
	if err := svc.DeployFlow(ctx, "flow-edge-only", 1, `{}`, "", "edge-fab2"); err != nil {
		t.Fatalf("DeployFlow: %v", err)
	}

	recvCtx, recvCancel := context.WithTimeout(ctx, 2*time.Second)
	defer recvCancel()
	cmd, err := edgeStream.Recv()
	if err != nil {
		t.Fatalf("rt-edge should have received the group-targeted deploy: %v", err)
	}
	if cmd.GetFlowId() != "flow-edge-only" {
		t.Fatalf("rt-edge received = %+v, want flow-edge-only", cmd)
	}

	// rt-server must NOT receive it — assert by racing a short timeout
	// against Recv, since there is no positive "nothing arrived" signal.
	done := make(chan struct{})
	go func() {
		_, _ = serverStream.Recv()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("rt-server (ungrouped) should not have received an edge-fab2-targeted deploy")
	case <-recvCtx.Done():
		// expected: nothing arrived within the timeout
	}
}
