package endpoints

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type WSConn struct {
	ws     *websocket.Conn
	sendCh chan []byte
	closed chan struct{}
	port   int
}

func NewWSConn(ws *websocket.Conn) *WSConn {
	c := &WSConn{
		ws:     ws,
		sendCh: make(chan []byte, 64),
		closed: make(chan struct{}),
		port:   4001,
	}
	go c.writePump()
	return c
}

func (c *WSConn) writePump() {
	for {
		select {
		case b, ok := <-c.sendCh:
			if !ok {
				return
			}
			log.Printf("writePump: writing message %s", string(b))
			if err := c.ws.WriteMessage(websocket.TextMessage, b); err != nil {
				log.Printf("writePump: WriteMessage error: %v", err)
				return
			}
			log.Printf("writePump: message written successfully")
		case <-c.closed:
			return
		}
	}
}

func (c *WSConn) SendJSON(msgType string, payload interface{}) error {
	out := map[string]interface{}{
		"type":    msgType,
		"ts":      time.Now().Unix(),
		"payload": payload,
	}
	b, err := json.Marshal(out)
	if err != nil {
		return err
	}
	log.Printf("SendJSON: queuing message %s", string(b))
	c.sendCh <- b
	return nil
}

func (c *WSConn) SendText(b []byte) {
	c.sendCh <- b
}

func (c *WSConn) Close() error {
	select {
	case <-c.closed:
		return nil
	default:
		close(c.closed)
		close(c.sendCh)
		return c.ws.Close()
	}
}
