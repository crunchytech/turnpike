// Copyright (c) 2013 Joshua Elliott
// Released under the MIT License
// http://opensource.org/licenses/MIT

package turnpike

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"

	"golang.org/x/net/websocket"
)

const (
	wampProtocolId = "wamp"
)

var clientBacklog = 10

// Client represents a WAMP client that handles RPC and pub/sub.
type Client struct {
	// SessionId is a ID of the session in UUID4 format received at the start of the session.
	SessionId string
	// ProtocolVersion is the version of the WAMP protocol received at the start of the session.
	ProtocolVersion int
	// ServerIdent is the server ID (ie "turnpike, autobahn") received at the start of the session.
	ServerIdent         string
	ws                  *websocket.Conn
	messages            chan string
	prefixes            prefixMap
	eventHandlers       map[string]EventHandler
	errorHandler        ErrorHandler
	calls               map[string]chan CallResult
	sessionOpenCallback func(string)
}

// CallResult represents either a sucess or a failure after a RPC call.
type CallResult struct {
	// Result contains the RPC call result returned by the server.
	Result interface{}
	// Error is nil on call success otherwise it contains the RPC error.
	Error error
}

// EventHandler is an interface for handlers to published events. The topicURI
// is the URI of the event and event is the event centents.
type EventHandler func(topicURI string, event interface{})

// ErrorHandler is an interface for handlers to pipe errors and disconnect
//	occurances to the application using Wamp
type ErrorHandler func(err error, disconnect bool)

// NewClient creates a new WAMP client.
func NewClient() *Client {
	return &Client{
		messages:      make(chan string, clientBacklog),
		prefixes:      make(prefixMap),
		eventHandlers: make(map[string]EventHandler),
		calls:         make(map[string]chan CallResult),
	}
}

// Prefix sets a CURIE prefix at the server for later use when interacting with
// the server. prefix is the first part of a CURIE (ie "calc") and URI is a full
// identifier (ie "http://example.com/simple/calc#") that is mapped to the prefix.
//
// Ref: http://wamp.ws/spec#prefix_message
func (c *Client) Prefix(prefix, URI string) error {
	if debug {
		log.Print("turnpike: sending prefix")
	}
	err := c.prefixes.registerPrefix(prefix, URI)
	if err != nil {
		return fmt.Errorf("turnpike: %s", err)
	}
	msg, err := createPrefix(prefix, URI)
	if err != nil {
		return fmt.Errorf("turnpike: %s", err)
	}
	c.messages <- string(msg)
	return nil
}

// Call makes a RPC call on the server identified by procURI (in either full URI
// or CURIE format) with zero or more args. Returns a channel that will receive
// the call result (or error) on completion.
//
// Ref: http://wamp.ws/spec#call_message
func (c *Client) Call(procURI string, args ...interface{}) chan CallResult {
	if debug {
		log.Print("turnpike: sending call")
	}
	// Channel size must be 1 to avoid blocking if no one is receiving the channel later.
	resultCh := make(chan CallResult, 1)
	callId := newId(16)
	msg, err := createCall(callId, procURI, args...)
	if err != nil {
		r := CallResult{
			Result: nil,
			Error:  fmt.Errorf("turnpike: %s", err),
		}
		resultCh <- r
		return resultCh
	}
	c.calls[callId] = resultCh
	c.messages <- string(msg)
	return resultCh
}

// Subscribe adds a subscription at the server for events with topicURI lasting
// for the session or until Unsubscribe is called.
//
// Ref: http://wamp.ws/spec#subscribe_message
func (c *Client) Subscribe(topicURI string, f EventHandler) error {
	if debug {
		log.Print("turnpike: sending subscribe")
	}
	msg, err := createSubscribe(topicURI)
	if err != nil {
		return fmt.Errorf("turnpike: %s", err)
	}
	c.messages <- string(msg)
	if f != nil {
		c.eventHandlers[topicURI] = f
	}
	return nil
}

// Unsubscribe removes a previous subscription with topicURI at the server.
//
// Ref: http://wamp.ws/spec#unsubscribe_message
func (c *Client) Unsubscribe(topicURI string) error {
	if debug {
		log.Print("turnpike: sending unsubscribe")
	}
	msg, err := createUnsubscribe(topicURI)
	if err != nil {
		return fmt.Errorf("turnpike: %s", err)
	}
	c.messages <- string(msg)
	delete(c.eventHandlers, topicURI)
	return nil
}

// Publish publishes an event to the topicURI that gets sent to all subscribers
// of that topicURI by the server. opts can can be either empty, one boolean
// that can be used to exclude outself from receiving the event or two lists;
// the first a list of clients to exclude and the second a list of clients that
// are eligible to receive the event. Either list can be empty.
//
// Ref: http://wamp.ws/spec#publish_message
func (c *Client) Publish(topicURI string, event interface{}, opts ...interface{}) error {
	if debug {
		log.Print("turnpike: sending publish")
	}
	msg, err := createPublish(topicURI, event, opts...)
	if err != nil {
		return fmt.Errorf("turnpike: %s", err)
	}
	c.messages <- string(msg)
	return nil
}

// PublishExcludeMe is a short hand for Publish(tobicURI, event, true) that will
// not send the event to ourself.
func (c *Client) PublishExcludeMe(topicURI string, event interface{}) error {
	return c.Publish(topicURI, event, true)
}

func (c *Client) handleCallResult(msg callResultMsg) {
	if debug {
		log.Print("turnpike: handling call result message")
	}
	resultCh, ok := c.calls[msg.CallID]
	if !ok {
		if debug {
			log.Print("turnpike: missing call result handler")
		}
		return
	}
	delete(c.calls, msg.CallID)
	r := CallResult{
		Result: msg.Result,
		Error:  nil,
	}
	resultCh <- r
}

func (c *Client) handleCallError(msg callErrorMsg) {
	if debug {
		log.Print("turnpike: handling call error message")
	}
	resultCh, ok := c.calls[msg.CallID]
	if !ok {
		if debug {
			log.Print("turnpike: missing call result handler")
		}
		return
	}
	delete(c.calls, msg.CallID)
	r := CallResult{
		Error: RPCError{
			URI:         msg.ErrorURI,
			Description: msg.ErrorDesc,
			Details:     msg.ErrorDetails,
		},
	}
	resultCh <- r
}

func (c *Client) handleEvent(msg eventMsg) {
	if debug {
		log.Print("turnpike: handling event message")
	}
	if f, ok := c.eventHandlers[msg.TopicURI]; ok && f != nil {
		f(msg.TopicURI, msg.Event)
	} else {
		if debug {
			log.Printf("turnpike: missing event handler for URI: %s", msg.TopicURI)
		}
		err := fmt.Errorf("turnpike: missing event handler for URI: %s", msg.TopicURI)
		c.handleError(err, false)
	}
}

//Simple handler to forward to package user, notifiying of disconnects as well
func (c *Client) handleError(err error, disconnect bool) {
	handler := c.errorHandler
	log.Println("About to call error handler...")
	handler(err, disconnect)
}

func (c *Client) receiveWelcome() error {
	if debug {
		log.Print("turnpike: receive welcome")
	}
	var rec string
	err := websocket.Message.Receive(c.ws, &rec)
	if err != nil {
		return fmt.Errorf("Error receiving welcome message: %s", err)
	}
	if typ := parseMessageType(rec); typ != msgWelcome {
		return fmt.Errorf("First message received was not welcome")
	}
	var msg welcomeMsg
	err = json.Unmarshal([]byte(rec), &msg)
	if err != nil {
		return fmt.Errorf("Error unmarshalling welcome message: %s", err)
	}
	c.SessionId = msg.SessionId
	c.ProtocolVersion = msg.ProtocolVersion
	c.ServerIdent = msg.ServerIdent
	if debug {
		log.Print("turnpike: session id: %s", c.SessionId)
		log.Print("turnpike: protocol version: %d", c.ProtocolVersion)
		log.Print("turnpike: server ident: %s", c.ServerIdent)
	}

	if c.sessionOpenCallback != nil {
		c.sessionOpenCallback(c.SessionId)
	}

	return nil
}

func (c *Client) receive() {
	for {
		var rec string
		err := websocket.Message.Receive(c.ws, &rec)
		if err != nil {
			if err != io.EOF {
				if debug {
					log.Printf("turnpike: error receiving message, aborting connection: %s", err)
				}
			}
			if debug {
				log.Printf("turnpike: breaking out of receive routine. Error: %s", err)
			}
			c.messages <- "KILLSENDROUTINE"
			c.handleError(err, true) //Pipe the error up to the calling application and indicate the disconnect
			break
		}
		if debug {
			log.Printf("turnpike: message received: %s", rec)
		}

		data := []byte(rec)

		switch typ := parseMessageType(rec); typ {
		case msgCallResult:
			var msg callResultMsg
			err := json.Unmarshal(data, &msg)
			if err != nil {
				if debug {
					log.Printf("turnpike: error unmarshalling call result message: %s", err)
				}
				continue
			}
			c.handleCallResult(msg)
		case msgCallError:
			var msg callErrorMsg
			err := json.Unmarshal(data, &msg)
			if err != nil {
				if debug {
					log.Printf("turnpike: error unmarshalling call error message: %s", err)
				}
				continue
			}
			c.handleCallError(msg)
		case msgEvent:
			var msg eventMsg
			err := json.Unmarshal(data, &msg)
			if err != nil {
				if debug {
					log.Printf("turnpike: error unmarshalling event message: %s", err)
				}
				continue
			}
			c.handleEvent(msg)
		case msgPrefix, msgCall, msgSubscribe, msgUnsubscribe, msgPublish:
			if debug {
				log.Printf("turnpike: client -> server message received, ignored: %s", messageTypeString(typ))
			}
		case msgWelcome:
			if debug {
				log.Print("turnpike: received extraneous welcome message, ignored")
			}
		default:
			if debug {
				log.Printf("turnpike: invalid message format, message dropped: %s", data)
			}
		}
	}
}

func (c *Client) send() {
	for msg := range c.messages {
		if msg == "KILLSENDROUTINE" {
			log.Printf("turnpike: send routine abort message recieved...breaking...")
			break
		}

		if debug {
			log.Printf("turnpike: sending message: %s", msg)
		}
		if err := websocket.Message.Send(c.ws, msg); err != nil {
			if debug {
				log.Printf("turnpike: error sending message: %s", err)
			}
			c.handleError(err, false)
		}
	}
}

// Connect will connect to server with an optional origin.
// More details here: http://godoc.org/code.google.com/p/go.net/websocket#Dial
func (c *Client) Connect(server, origin string, f ErrorHandler) error {
	if debug {
		log.Print("turnpike: connect")
	}
	var err error
	if c.ws, err = websocket.Dial(server, wampProtocolId, origin); err != nil {
		return fmt.Errorf("Error connecting to websocket server: %s", err)
	}

	if f != nil {
		c.errorHandler = f
	}

	// Receive welcome message
	if err = c.receiveWelcome(); err != nil {
		return err
	}
	if debug {
		log.Printf("turnpike: connected to server: %s", server)
	}

	go c.receive()
	go c.send()

	return nil
}

// Ping is a simple Write to the websocket connection to test if the connection is alive
func (c *Client) Ping(topicURI string) error {

	var err error
	var msg []byte //empty message

	if c.ws == nil {
		return fmt.Errorf("Error: websocket connection is nil")
	}
	c.ws.PayloadType = 9 //Set payload type to PingFrame (since we're pinging the server)

	if _, err = c.ws.Write(msg); err != nil {
		return fmt.Errorf("Error writing Ping to websocket: %s", err)
	}

	if c.ws == nil {
		return fmt.Errorf("Error: websocket connection is nil")
	}
	c.ws.PayloadType = 1 //Set the PayloadType back to TextFrame to resume normal use
	return nil
}

// SetSessionOpenCallback adds a callback function that is run when a new session begins.
// The callback function must accept a string argument that is the session ID.
func (c *Client) SetSessionOpenCallback(f func(string)) {
	c.sessionOpenCallback = f
}

// newId generates a random string of fixed size.
func newId(size int) string {
	const alpha = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz0123456789-_"
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = alpha[rand.Intn(len(alpha))]
	}
	return string(buf)
}
