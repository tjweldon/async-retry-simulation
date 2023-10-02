package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "")
	if r.URL.Query().Has("rate") {
		r_string := r.URL.Query().Get("rate")
		if s, err := strconv.ParseFloat(r_string, 64); err == nil {
			success_rate = s
			log.Println("new success rate: ", s)
			fmt.Fprintln(w, "new success rate: ", s)
		} else {
			log.Println("could not parse")
			fmt.Fprintln(w, "could not parse")
		}
	}
}

type Msg struct {
	UID string `json:"uid"`
}

type Ack struct {
	MSG Msg  `json:"msg"`
	OK  bool `josn:"ok"`
}

type Nack struct {
	REASON string `json:"reason"`
	OK     bool   `json:"OK"`
}

func read_one(conn *websocket.Conn) (ok bool) {
	msgType, msg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return false
	}

	switch msgType {
	case websocket.CloseMessage:
		log.Println("Closing socket")
		conn.Close()
		return false
	case websocket.BinaryMessage:
		log.Println("Rejecting binary message")
		resp := Nack{
			REASON: "No binary messages plz",
			OK:     false,
		}
		if err = conn.WriteJSON(&resp); err != nil {
			log.Println(err)
		}

	case websocket.TextMessage:
		incoming := Msg{}
		if err = json.Unmarshal(msg, &incoming); err != nil {
			log.Println(err)
		}

		ns := time.Now().Nanosecond()
		ok := float64(int(ns)%100) < success_rate

		resp := Ack{
			MSG: incoming,
			OK:  ok,
		}
		log.Printf("Acking text message: %s, -> %v", msg, resp)
		if err = conn.WriteJSON(&resp); err != nil {
			log.Println(err)
		}
	}

	return true
}

func reader(conn *websocket.Conn) {
	for ok := true; ok; ok = read_one(conn) {
	}
}

func wsEndpoint(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
	}

	reader(ws)
}

func setupRoutes() {
	http.HandleFunc("/", homePage)
	http.HandleFunc("/ws", wsEndpoint)
}

const port = ":8080"

var args = os.Args[1:]

var success_rate float64 = 1.0

func init() {
	if len(args) > 0 {
		if s, err := strconv.ParseFloat(args[0], 64); err == nil {
			success_rate = s
		} else {
			log.Println("could not parse ", args[0], ". Using: ", success_rate)
		}
	}
}

func main() {
	fmt.Println("Success rate set: ", success_rate)
	fmt.Println("Serving on localhost" + port)
	setupRoutes()
	log.Fatal(http.ListenAndServe(port, nil))
}
