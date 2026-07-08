// Package tcpshared is the shared state between "tcp-in" (server mode) and
// "tcp-out" (server mode): a per-addr Hub tracking currently connected
// clients so a "tcp-out" server-mode node in the same flow can broadcast to
// whatever "tcp-in" server-mode node accepted them (CON-290), mirroring
// engine/nodes/wsshared's websocket Hub.
package tcpshared

import (
	"net"
	"sync"
)

// Hub tracks the live connections accepted under one server addr.
type Hub struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newHub() *Hub { return &Hub{conns: map[net.Conn]struct{}{}} }

// Add registers a newly accepted connection.
func (h *Hub) Add(c net.Conn) {
	h.mu.Lock()
	h.conns[c] = struct{}{}
	h.mu.Unlock()
}

// Remove drops a connection (on disconnect).
func (h *Hub) Remove(c net.Conn) {
	h.mu.Lock()
	delete(h.conns, c)
	h.mu.Unlock()
}

// CloseAll force-closes every currently tracked connection (node shutdown).
func (h *Hub) CloseAll() {
	h.mu.Lock()
	conns := h.conns
	h.conns = map[net.Conn]struct{}{}
	h.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

// Broadcast writes data to every currently connected client, dropping (and
// removing) any connection whose write fails. Returns how many clients
// received it.
func (h *Hub) Broadcast(data []byte) int {
	h.mu.Lock()
	conns := make([]net.Conn, 0, len(h.conns))
	for c := range h.conns {
		conns = append(conns, c)
	}
	h.mu.Unlock()

	sent := 0
	for _, c := range conns {
		if _, err := c.Write(data); err != nil {
			h.Remove(c)
			continue
		}
		sent++
	}
	return sent
}

var (
	mu    sync.Mutex
	byKey = map[string]*Hub{}
)

// HubFor returns the shared Hub for a server addr, creating it on first
// use.
func HubFor(addr string) *Hub {
	mu.Lock()
	defer mu.Unlock()
	h, ok := byKey[addr]
	if !ok {
		h = newHub()
		byKey[addr] = h
	}
	return h
}
