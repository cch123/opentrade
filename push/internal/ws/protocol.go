// Package ws provides the WebSocket gateway: client handshake, read/write
// loops, and the JSON wire protocol.
package ws

import "encoding/json"

// Client → Server control messages.
type ClientMsg struct {
	Op      string   `json:"op"`                // "subscribe" | "unsubscribe" | "ping"
	Streams []string `json:"streams,omitempty"` // required for (un)subscribe
}

// Server → Client control / ack envelope (used for pongs, acks, errors).
type ControlMsg struct {
	Op      string   `json:"op"`
	Streams []string `json:"streams,omitempty"`
	Message string   `json:"message,omitempty"`
}

// Server → Client data envelope. Broadcast / private sends use this form.
type DataMsg struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// Fixed stream keys / op codes.
const (
	OpSubscribe   = "subscribe"
	OpUnsubscribe = "unsubscribe"
	OpPing        = "ping"
	OpPong        = "pong"
	OpAck         = "ack"
	OpError       = "error"

	// StreamUser is the implicit private stream. Every authenticated
	// connection is subscribed to it automatically.
	StreamUser = "user"

	// StreamPerpUser is the private perp (futures) stream — position /
	// margin / settlement / funding / liquidation updates from perp-journal
	// (ADR-0068 M7). Kept separate from StreamUser so clients can subscribe to
	// spot and perp private flows independently.
	StreamPerpUser = "perp-user"
)

// EncodeControl returns a pre-serialized control message.
func EncodeControl(m ControlMsg) ([]byte, error) { return json.Marshal(m) }

// EncodeData returns a pre-serialized data frame.
func EncodeData(stream string, data []byte) ([]byte, error) {
	return json.Marshal(DataMsg{Stream: stream, Data: data})
}
