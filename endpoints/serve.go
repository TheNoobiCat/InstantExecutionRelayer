package endpoints

import (
	"encoding/json"
	"io"
	"log"

	"github.com/gorilla/websocket"
	mp "github.com/thenoobicat/instantexecutionrelayer/messagepack"
	"github.com/thenoobicat/instantexecutionrelayer/relayer"
	"github.com/vmihailenco/msgpack/v5"
)

func HandleConnection(ws *websocket.Conn, relayer *relayer.Relayer) {
	conn := NewWSConn(ws)
	defer conn.Close()

	if relayer == nil {
		log.Println("Relayer not initialized")
		_ = conn.SendJSON("error", map[string]interface{}{
			"error": "Server configuration error",
		})
		return
	}

	for {
		msgType, reader, err := ws.NextReader()
		if err != nil {
			if err != io.EOF {
				log.Println("HandleConnection: read error:", err)
			}
			return
		}

		switch msgType {
		case websocket.BinaryMessage:
			log.Println("Received binary message")

			// Read all data first
			data, err := io.ReadAll(reader)
			if err != nil {
				log.Printf("Failed to read message data: %v", err)
				continue
			}

			log.Printf("Message data length: %d bytes", len(data))

			// Try to decode as SignedInstantExecute first
			var sie mp.SignedInstantExecute
			if err := msgpack.Unmarshal(data, &sie); err == nil {
				log.Printf("Decoded SignedInstantExecute: type='%s'", sie.Type)
				if sie.Type == "instant_execute" {
					HandleSignedInstantExecute(conn, &sie, relayer)
					continue
				}
			} else {
				log.Printf("Failed to decode as SignedInstantExecute: %v", err)
			}

			// Try to decode as SignedBatchInstantExecute
			var sbie mp.SignedBatchInstantExecute
			if err := msgpack.Unmarshal(data, &sbie); err == nil {
				log.Printf("Decoded SignedBatchInstantExecute: type='%s'", sbie.Type)
				if sbie.Type == "batch_instant_execute" {
					HandleSignedBatchInstantExecute(conn, &sbie, relayer)
					continue
				}
			} else {
				log.Printf("Failed to decode as SignedBatchInstantExecute: %v", err)
			}

			// Try to decode as session creation
			var ssc mp.SignedSessionCreation
			if err := msgpack.Unmarshal(data, &ssc); err == nil {
				log.Printf("Decoded SignedSessionCreation: type='%s'", ssc.Type)
				if ssc.Type == "session_create" {
					HandleSessionCreation(conn, &ssc, relayer)
					continue
				}
			} else {
				log.Printf("Failed to decode as SignedSessionCreation: %v", err)
			}

			// Try to decode as channel creation
			var scc mp.SignedChannelCreation
			if err := msgpack.Unmarshal(data, &scc); err == nil {
				log.Printf("Decoded SignedChannelCreation: type='%s'", scc.Type)
				if scc.Type == "channel_create" {
					HandleChannelCreation(conn, &scc, relayer)
					continue
				}
			} else {
				log.Printf("Failed to decode as SignedChannelCreation: %v", err)
			}

			// Try to decode as batch channel creation
			var bcc mp.BatchChannelCreation
			if err := msgpack.Unmarshal(data, &bcc); err == nil {
				log.Printf("Decoded BatchChannelCreation: type='%s'", bcc.Type)
				if bcc.Type == "batch_channel_create" {
					HandleBatchChannelCreation(conn, &bcc, relayer)
					continue
				}
			} else {
				log.Printf("Failed to decode as BatchChannelCreation: %v", err)
			}

			// Try to decode as gas estimation request
			var ger mp.GasEstimationRequest
			if err := msgpack.Unmarshal(data, &ger); err == nil {
				log.Printf("Decoded GasEstimationRequest: type='%s'", ger.Type)
				if ger.Type == "gas_estimation" {
					HandleGasEstimation(conn, &ger, relayer)
					continue
				}
			} else {
				log.Printf("Failed to decode as GasEstimationRequest: %v", err)
			}

			// Try generic decode to see what fields are present
			var generic map[string]interface{}
			if err := msgpack.Unmarshal(data, &generic); err == nil {
				log.Printf("Decoded generic msgpack: %+v", generic)
				if t, ok := generic["typ"].(string); ok {
					_ = conn.SendJSON("notice", map[string]string{"msg": "received msgpack message type=" + t})
					continue
				}
				if t, ok := generic["t"].(string); ok {
					_ = conn.SendJSON("notice", map[string]string{"msg": "received msgpack message type=" + t})
					continue
				}
			} else {
				log.Printf("Failed to decode as generic msgpack: %v", err)
			}

			log.Println("Unknown binary message")
			_ = conn.SendJSON("error", map[string]string{"msg": "unknown binary message"})

		case websocket.TextMessage:
			var j map[string]interface{}
			if err := json.NewDecoder(reader).Decode(&j); err == nil {
				if t, ok := j["type"].(string); ok {
					_ = conn.SendJSON("notice", map[string]string{"msg": "received text message type=" + t})
					continue
				}
			}
			_ = conn.SendJSON("error", map[string]string{"msg": "unknown text message"})

		default:
			_ = conn.SendJSON("error", map[string]string{"msg": "unsupported message type"})
		}
	}
}
