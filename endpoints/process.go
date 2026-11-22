package endpoints

import (
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
	mp "github.com/thenoobicat/instantexecutionrelayer/messagepack"
	"github.com/vmihailenco/msgpack/v5"
)

func Process(conn *websocket.Conn) {
	msgType, data, err := conn.ReadMessage()
	if err != nil {
		log.Println("endpoints.Process: read error:", err)
		_ = conn.WriteMessage(websocket.TextMessage, []byte("relayer: failed to read message"))
		return
	}

	log.Printf("Received message type: %d, data length: %d", msgType, len(data))

	switch msgType {
	case websocket.BinaryMessage:
		log.Println("Processing binary message...")

		var sie mp.SignedInstantExecute
		if err := msgpack.Unmarshal(data, &sie); err == nil {
			log.Printf("Decoded SignedInstantExecute: type=%s", sie.Type)
			if sie.Type == "instant_execute" {
				wconn := NewWSConn(conn)
				defer wconn.Close()
				HandleSignedInstantExecute(wconn, &sie, nil)
				return
			}
		}

		var ssc mp.SignedSessionCreation
		if err := msgpack.Unmarshal(data, &ssc); err == nil {
			log.Printf("Decoded SignedSessionCreation: type=%s", ssc.Type)
			if ssc.Type == "session_create" {
				wconn := NewWSConn(conn)
				defer wconn.Close()
				HandleSessionCreation(wconn, &ssc, nil)
				return
			}
		} else {
			log.Printf("Failed to decode as SignedSessionCreation: %v", err)
		}

		var generic map[string]interface{}
		if err := msgpack.Unmarshal(data, &generic); err == nil {
			log.Printf("Decoded generic msgpack: %+v", generic)
			if t, ok := generic["typ"].(string); ok {
				_ = conn.WriteMessage(websocket.TextMessage, []byte("relayer: received msgpack message type="+t))
				return
			}
			if t, ok := generic["t"].(string); ok {
				_ = conn.WriteMessage(websocket.TextMessage, []byte("relayer: received msgpack message type="+t))
				return
			}
		} else {
			log.Printf("Failed to decode as generic msgpack: %v", err)
		}

		log.Println("Unknown binary message")
		_ = conn.WriteMessage(websocket.TextMessage, []byte("relayer: unknown binary message"))

	case websocket.TextMessage:
		var j map[string]interface{}
		if err := json.Unmarshal(data, &j); err == nil {
			if t, ok := j["type"].(string); ok {
				_ = conn.WriteMessage(websocket.TextMessage, []byte("relayer: received text message type="+t))
				return
			}
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte("relayer: unknown text message"))

	default:
		_ = conn.WriteMessage(websocket.TextMessage, []byte("relayer: unsupported message type"))
	}
}
