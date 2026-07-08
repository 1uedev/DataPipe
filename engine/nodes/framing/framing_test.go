package framing

import (
	"context"
	"testing"
	"time"
)

// queuePoll turns a fixed sequence of byte chunks into a PollFunc: each
// call returns the next queued chunk (or (0, nil) once exhausted, so Run's
// caller-side loop can decide when to stop via ctx).
func queuePoll(chunks [][]byte) (PollFunc, func()) {
	i := 0
	stop := make(chan struct{})
	return func(buf []byte) (int, error) {
		if i < len(chunks) {
			n := copy(buf, chunks[i])
			i++
			return n, nil
		}
		select {
		case <-stop:
			return 0, context.Canceled
		case <-time.After(2 * time.Millisecond):
			return 0, nil
		}
	}, func() { close(stop) }
}

func TestCON290_DelimiterFraming(t *testing.T) {
	poll, stop := queuePoll([][]byte{[]byte("hello\nworld\npart")})
	defer stop()
	var frames []string
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := Run(ctx, poll, Config{Mode: "delimiter", Delimiter: "\\n"}, func(f []byte) error {
		frames = append(frames, string(f))
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(frames) != 2 || frames[0] != "hello" || frames[1] != "world" {
		t.Errorf("frames = %+v", frames)
	}
}

func TestCON290_FixedLengthFraming(t *testing.T) {
	poll, stop := queuePoll([][]byte{[]byte("AABBCC")})
	defer stop()
	var frames []string
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := Run(ctx, poll, Config{Mode: "fixedLength", Length: 2}, func(f []byte) error {
		frames = append(frames, string(f))
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(frames) != 3 || frames[0] != "AA" || frames[2] != "CC" {
		t.Errorf("frames = %+v", frames)
	}
}

func TestCON290_LengthPrefixFraming(t *testing.T) {
	// 2-byte big-endian length prefix: [0,3]"abc" then [0,2]"de"
	poll, stop := queuePoll([][]byte{{0, 3, 'a', 'b', 'c', 0, 2, 'd', 'e'}})
	defer stop()
	var frames []string
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := Run(ctx, poll, Config{Mode: "lengthPrefix", LengthPrefixBytes: 2}, func(f []byte) error {
		frames = append(frames, string(f))
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(frames) != 2 || frames[0] != "abc" || frames[1] != "de" {
		t.Errorf("frames = %+v", frames)
	}
}

func TestCON290_TimeoutFramingFlushesAfterIdle(t *testing.T) {
	poll, stop := queuePoll([][]byte{[]byte("partial")})
	defer stop()
	var frames []string
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := Run(ctx, poll, Config{Mode: "timeout", TimeoutMs: 20}, func(f []byte) error {
		frames = append(frames, string(f))
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(frames) != 1 || frames[0] != "partial" {
		t.Errorf("frames = %+v", frames)
	}
}

func TestCON290_MaxFrameBytesBoundsUnboundedBuffering(t *testing.T) {
	big := make([]byte, 100)
	poll, stop := queuePoll([][]byte{big, big, big})
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := Run(ctx, poll, Config{Mode: "delimiter", Delimiter: "\\n", MaxFrameBytes: 150}, func([]byte) error { return nil })
	if err == nil {
		t.Fatal("expected an error when the buffer exceeds MaxFrameBytes without a delimiter")
	}
}

func TestCON290_ValidateRejectsBadConfig(t *testing.T) {
	cases := []Config{
		{Mode: "delimiter"},
		{Mode: "fixedLength"},
		{Mode: "lengthPrefix", LengthPrefixBytes: 3},
		{Mode: "timeout"},
		{Mode: "bogus"},
	}
	for _, c := range cases {
		if err := c.Validate(); err == nil {
			t.Errorf("Validate(%+v): expected an error", c)
		}
	}
}
