// Package wsshared is the shared state between "websocket-in" (server mode)
// and "websocket-out" (server mode): a per-path Hub tracking currently
// connected clients so a "websocket-out" server-mode node in the same flow
// can broadcast to whatever "websocket-in" server-mode node accepted them
// (CON-320).
package wsshared

import (
	"sync"

	"github.com/gorilla/websocket"
)

// Hub tracks the live connections accepted under one server path.
type Hub struct {
	mu    sync.Mutex
	conns map[*websocket.Conn]struct{}
}

func newHub() *Hub { return &Hub{conns: map[*websocket.Conn]struct{}{}} }

// Add registers a newly accepted connection.
func (h *Hub) Add(c *websocket.Conn) {
	h.mu.Lock()
	h.conns[c] = struct{}{}
	h.mu.Unlock()
}

// Remove drops a connection (on disconnect).
func (h *Hub) Remove(c *websocket.Conn) {
	h.mu.Lock()
	delete(h.conns, c)
	h.mu.Unlock()
}

// CloseAll force-closes every currently tracked connection (node shutdown):
// each accepting node's blocking read loop then sees its ReadMessage error
// out and can return, rather than a shutdown having to wait for the peer to
// send another frame or a close handshake.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	conns := h.conns
	h.conns = map[*websocket.Conn]struct{}{}
	h.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

// Broadcast sends messageType/data to every currently connected client,
// dropping (and removing) any connection whose write fails. Returns how
// many clients received it.
func (h *Hub) Broadcast(messageType int, data []byte) int {
	h.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(h.conns))
	for c := range h.conns {
		conns = append(conns, c)
	}
	h.mu.Unlock()

	sent := 0
	for _, c := range conns {
		if err := c.WriteMessage(messageType, data); err != nil {
			h.Remove(c)
			continue
		}
		sent++
	}
	return sent
}

// registry is the process-wide path -> Hub map, mirroring
// engine/webhook.DefaultRegistry's "one process-wide state, looked up by
// path" pattern — paths are already required to be globally unique on the
// shared webhook HTTP server, so a Hub can be keyed by path alone.
var (
	mu    sync.Mutex
	byKey = map[string]*Hub{}
)

// HubFor returns the shared Hub for a server path, creating it on first
// use. A "websocket-out" node in mode "server" broadcasts through the same
// Hub a "websocket-in" node in mode "server" accepted connections into.
func HubFor(path string) *Hub {
	mu.Lock()
	defer mu.Unlock()
	h, ok := byKey[path]
	if !ok {
		h = newHub()
		byKey[path] = h
	}
	return h
}
