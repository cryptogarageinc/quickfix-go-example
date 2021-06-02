package websocket

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

var (
	newline = []byte{'\n'}
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
		c.hub.unregister <- c
	}()

	for {
		select {
		case message, ok := <-c.send:
			fmt.Println("message incoming")
			fmt.Printf("message: '%s'\n", message)
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
	w, err := c.conn.NextWriter(websocket.TextMessage)
	if err != nil {
		fmt.Println("error next writer")
		return
	}
			w.Write(message)
			// Add queued chat messages to the current websocket message.
//			n := len(c.send)
//			fmt.Printf("queue length: '%d'\n", n)
//			for i := 0; i < n; i++ {
//				w.Write(newline)
//				w.Write(<-c.send)
//			}

			if err := w.Close(); err != nil {
				fmt.Println("error close writer")
				return
			}
			fmt.Println("message sent")

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				fmt.Println("haven't recieved any return")
				return
			}
		}
	}
}

func ServeWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	fmt.Println("request incoming")
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	client := &Client{hub: hub, conn: conn, send: make(chan []byte, 256)}
	client.hub.register <- client

	go client.writePump()
}
