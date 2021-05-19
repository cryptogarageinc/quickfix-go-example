package websocket

import "fmt"

type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	Broadcast  chan []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		Broadcast:  make(chan []byte),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			fmt.Println("unregister a client")
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
		case message := <-h.Broadcast:
			fmt.Println("broadcast")
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					fmt.Println("close connection")
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}
