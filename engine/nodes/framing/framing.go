// Package framing implements CON-290's raw-byte-stream framing options
// (delimiter, fixed length, length prefix, timeout/idle-flush) shared by
// "tcp-in" and "serial-in". UDP needs no framing — each datagram socket
// read is already one discrete message.
package framing

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

// Config configures how a raw byte stream is split into frames.
type Config struct {
	Mode                   string `json:"mode"` // "delimiter" | "fixedLength" | "lengthPrefix" | "timeout"
	Delimiter              string `json:"delimiter,omitempty"`
	Length                 int    `json:"length,omitempty"`
	LengthPrefixBytes      int    `json:"lengthPrefixBytes,omitempty"`      // 1, 2, or 4
	LengthPrefixEndianness string `json:"lengthPrefixEndianness,omitempty"` // "big" (default) | "little"
	TimeoutMs              int    `json:"timeoutMs,omitempty"`
	MaxFrameBytes          int    `json:"maxFrameBytes,omitempty"` // guards against unbounded buffering (BUS-110); default 1 MiB
}

// DefaultMaxFrameBytes bounds how large a single accumulated frame may grow
// before Run reports an error, so a stream that never produces a delimiter/
// terminates a length-prefixed frame can't buffer without limit.
const DefaultMaxFrameBytes = 1 << 20

// Validate checks cfg is internally consistent for its Mode.
func (cfg Config) Validate() error {
	switch cfg.Mode {
	case "delimiter":
		if cfg.Delimiter == "" {
			return fmt.Errorf("framing: delimiter is required for mode \"delimiter\"")
		}
	case "fixedLength":
		if cfg.Length <= 0 {
			return fmt.Errorf("framing: length must be positive for mode \"fixedLength\"")
		}
	case "lengthPrefix":
		switch cfg.LengthPrefixBytes {
		case 1, 2, 4:
		default:
			return fmt.Errorf("framing: lengthPrefixBytes must be 1, 2, or 4, got %d", cfg.LengthPrefixBytes)
		}
	case "timeout":
		if cfg.TimeoutMs <= 0 {
			return fmt.Errorf("framing: timeoutMs must be positive for mode \"timeout\"")
		}
	default:
		return fmt.Errorf("framing: mode must be \"delimiter\", \"fixedLength\", \"lengthPrefix\", or \"timeout\", got %q", cfg.Mode)
	}
	return nil
}

// PollFunc reads into buf, returning (n>0, nil) for data received, (0, nil)
// for "no data within this short poll" (lets Run check ctx/idle timers), or
// a non-nil error to abort (a real I/O error, or ctx cancellation surfaced
// by the underlying transport).
type PollFunc func(buf []byte) (int, error)

// Run polls for bytes and emits each extracted frame to onFrame, until ctx
// is cancelled or poll/onFrame returns an error.
func Run(ctx context.Context, poll PollFunc, cfg Config, onFrame func([]byte) error) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	delim := unescapeDelimiter(cfg.Delimiter)
	maxFrame := cfg.MaxFrameBytes
	if maxFrame <= 0 {
		maxFrame = DefaultMaxFrameBytes
	}

	var buf []byte
	chunk := make([]byte, 4096)
	lastData := time.Now()
	for {
		if ctx.Err() != nil {
			return nil
		}
		n, err := poll(chunk)
		if err != nil {
			return err
		}
		if n > 0 {
			buf = append(buf, chunk[:n]...)
			lastData = time.Now()
			if len(buf) > maxFrame {
				return fmt.Errorf("framing: accumulated %d bytes without a complete frame (limit %d)", len(buf), maxFrame)
			}
		}

		for {
			frame, rest, ok, extractErr := extract(cfg, buf, delim)
			if extractErr != nil {
				return extractErr
			}
			if !ok {
				break
			}
			if err := onFrame(frame); err != nil {
				return err
			}
			buf = rest
		}

		if cfg.Mode == "timeout" && len(buf) > 0 && time.Since(lastData) >= time.Duration(cfg.TimeoutMs)*time.Millisecond {
			if err := onFrame(buf); err != nil {
				return err
			}
			buf = nil
		}
	}
}

func extract(cfg Config, buf, delim []byte) (frame, rest []byte, ok bool, err error) {
	switch cfg.Mode {
	case "delimiter":
		idx := bytes.Index(buf, delim)
		if idx < 0 {
			return nil, buf, false, nil
		}
		return buf[:idx], buf[idx+len(delim):], true, nil
	case "fixedLength":
		if len(buf) < cfg.Length {
			return nil, buf, false, nil
		}
		return buf[:cfg.Length], buf[cfg.Length:], true, nil
	case "lengthPrefix":
		n := cfg.LengthPrefixBytes
		if len(buf) < n {
			return nil, buf, false, nil
		}
		order := byteOrder(cfg.LengthPrefixEndianness)
		var length int
		switch n {
		case 1:
			length = int(buf[0])
		case 2:
			length = int(order.Uint16(buf))
		case 4:
			length = int(order.Uint32(buf))
		}
		if len(buf) < n+length {
			return nil, buf, false, nil
		}
		return buf[n : n+length], buf[n+length:], true, nil
	default: // "timeout" — extraction happens via the idle check in Run, not here
		return nil, buf, false, nil
	}
}

func byteOrder(endianness string) binary.ByteOrder {
	if endianness == "little" {
		return binary.LittleEndian
	}
	return binary.BigEndian
}

// unescapeDelimiter turns common literal escape sequences ("\n", "\r",
// "\t") into their actual bytes, so a config field can say "\n" rather than
// requiring a raw newline in JSON.
func unescapeDelimiter(s string) []byte {
	s = strings.ReplaceAll(s, `\n`, "\n")
	s = strings.ReplaceAll(s, `\r`, "\r")
	s = strings.ReplaceAll(s, `\t`, "\t")
	return []byte(s)
}
