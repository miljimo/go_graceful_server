package tcp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"time"

	"github.com/miljimo/logger"
	"github.com/miljimo/logger/watcher"
)

const (
	//Server channel operations constants
	cServerStartOperation                       = "cServerStartOperation"
	cServerShutDownOperation                    = "cServerShutDownOperation"
	cRemoveAndShutdownClientConnectionOperation = "cRemoveAndShutdownClientConnectionOperation"
	cServerLoopShutDownOperation                = "cServerLoopShutDownOperation"
	cCreateServerConnectionOperation            = "cCreateServerConnectionOperation"

	//Connection Request Methods
	cCreateKvsItemRequest = "cCreateKvsItemRequest"
	cDeleteKvsItemRequest = "cDeleteKvsItemRequest"
	cUpdateKvsItemRequest = "cUpdateKvsItemRequest"
	cGetKvsItemRequest    = "cGetKvsItemRequest"
	cGetAllKvsItemRequest = "cGetAllKvsItemRequest"
)

const (
	PORT_ADDRESS            int = 8002
	MAX_CONNECTION_CAPACITY int = 1000
	HANDLE_REQUEST_TIMEOUT      = time.Second * 5
)

type Server struct {
	listener     net.Listener
	watcher      *watcher.Watcher
	shuttingDown bool
	created      bool
	started      bool
	shutdownC    chan struct{}
	quitC        chan struct{}
	acceptC      chan *Connection
	connections  map[string]Connection
}

type Connection struct {
	client             net.Conn
	connectedTimestamp time.Time
	ipAddress          string
	connected          bool
	server             *Server
	id                 string
	quitC              chan struct{}
}

type serverEventMessage struct {
	nativeConn net.Conn
	server     *Server
	conn       *Connection
}

type ServerOperationResult struct {
	conn *Connection
}

type Request struct {
	Body io.ReadCloser
}

type Response struct {
	Body io.Writer
}

var Handler func(r *Response, request *Request)

func createServerConnection(server *Server, conn net.Conn) *Connection {
	message := serverEventMessage{server: server, nativeConn: conn}
	channel, err := watcher.Send(server.watcher, cCreateServerConnectionOperation, message)
	if err != nil {
		logger.Warning("createServerConnection", err.Error())
	}
	result := (<-channel).(ServerOperationResult)
	return result.conn
}

func notifyServerClientMessageLoopTerminated(server *Server, conn *Connection) {
	message := serverEventMessage{server: server, conn: conn}
	channel, err := watcher.Send(server.watcher, cRemoveAndShutdownClientConnectionOperation, message)
	if err != nil {
		logger.Warning("notifyServerClientMessageLoopTerminated", err.Error())
		return
	}
	<-channel
}

func tellServerClientLoopHasBeenTerminated(server *Server) {
	message := serverEventMessage{server: server, conn: nil}
	channel, err := watcher.Send(server.watcher, cServerLoopShutDownOperation, message)
	if err != nil {
		logger.Warning("tellServerClientLoopHasBeenTerminated", err.Error())
		return
	}
	<-channel
}

func processServerQueueOperations(op watcher.Operation) {

	message := op.Item.(serverEventMessage)
	result := ServerOperationResult{}

	switch op.Type {

	case cRemoveAndShutdownClientConnectionOperation:

		if message.conn != nil {
			logger.Information(op.Type, fmt.Sprintf("TCP Server terminating client %s ", message.conn.ipAddress))

			if len(message.server.connections) > 0 {
				delete(message.server.connections, message.conn.id)
			}
			message.conn.client.Close()
			logger.Debug(op.Type, fmt.Sprintf("TCP client connection %s as be closed ", message.conn.ipAddress))
		}

		if message.server.shuttingDown {

			if len(message.server.connections) == 0 {
				watcher.Stop(message.server.watcher)
				message.server.started = false
			}
		}

		op.Channel <- result

	case cServerShutDownOperation:
		message.server.shuttingDown = true
		close(message.server.acceptC)
		close(message.server.quitC)
		op.Channel <- result

	case cCreateServerConnectionOperation:
		if message.nativeConn != nil {
			logger.Information(op.Type, fmt.Sprintf("adding new TCP connection %s", message.nativeConn.LocalAddr()))
			acceptedDateTime := time.Now()
			var index int64 = int64(len(message.server.connections))
			client := Connection{client: message.nativeConn,
				server:             message.server,
				connectedTimestamp: acceptedDateTime}
			client.ipAddress = message.nativeConn.RemoteAddr().String()
			client.connected = true
			client.quitC = make(chan struct{})
			client.id = strconv.FormatInt(index, 10)
			message.server.connections[client.id] = client
			result.conn = &client
			logger.Debug(op.Type, fmt.Sprintf("Total TCP connection(s) in processing queue %d", len(message.server.connections)))
		}
		op.Channel <- result

	case cServerStartOperation:
		if message.server.started {
			logger.Information(op.Type, fmt.Sprintf("TCP Server started  already  at : %s", message.server.listener.Addr()))
			return
		}
		logger.Information(op.Type, fmt.Sprintf("Starting  TCP Server at : %s", message.server.listener.Addr()))
		message.server.started = true
		message.server.shuttingDown = false
		acceptNewConnectionFromOutsideWorldInBackground(message.server)
		processRecieveChannelConnectionInbackground(message.server)
		op.Channel <- result
	}
}

func acceptNewConnectionFromOutsideWorldInBackground(server *Server) {

	go func() {
		defer func() {
			err := recover()
			if err != nil {
				logger.Error("acceptNewConnectionFromOutsideWorldInBackground", err.(error))
			}
			tellServerClientLoopHasBeenTerminated(server)
		}()
		//non-blocking accept
		acceptC := acceptConnectionFromOutsideWorldInbackground(server)

		for {
			select {

			case <-server.quitC:
				return
			case <-acceptC:
				acceptC = acceptConnectionFromOutsideWorldInbackground(server)
			default:
				runtime.Gosched()

			}
		}

	}()

}

func acceptConnectionFromOutsideWorldInbackground(server *Server) <-chan struct{} {

	acceptedC := make(chan struct{})

	go func() {

		defer func() {

			err := recover()
			if err != nil {
				logger.Error("acceptConnectionFromOutsideWorldInbackground", err.(error))
			}
			close(acceptedC)
		}()

		logger.Debug("acceptConnectionFromOutsideWorldInbackground", "Waiting to accept  TCP connection...")
		conn, err := server.listener.Accept()
		if err != nil {
			logger.Error("acceptConnectionFromOutsideWorldInbackground", err)
			return
		}

		if server.shuttingDown {
			logger.Error("acceptConnectionFromOutsideWorldInbackground", errors.New("@acceptConnection: TCP server is shutting down"))
			return
		}

		connWrapper := createServerConnection(server, conn)
		server.acceptC <- connWrapper
		acceptedC <- struct{}{}

	}()

	return acceptedC
}

func processRecieveChannelConnectionInbackground(server *Server) {
	//create a roles here
	if server == nil {
		panic("TCP Server object cannot be a nil")
	}
	if !server.created {
		panic("TCP Server object has to be create, using the CreateTCPServer api")
	}

	go func() {

		for conn := range server.acceptC {
			c := conn
			logger.Information("processRecieveChannelConnectionInbackground", fmt.Sprintf("TCP connection channel recieved from %s ", c.ipAddress))
			go startClientAndServerInfiniteCommunicationLoop(c)

		}
	}()
}

func startClientAndServerInfiniteCommunicationLoop(conn *Connection) {

	defer func() {
		err := recover()
		if err != nil {
			logger.Error("processRecieveChannelConnectionInbackground", err.(error))
		}
		//notify the server that connection  loop terminated
		notifyServerClientMessageLoopTerminated(conn.server, conn)
	}()

	processC := processClientRequestAndItsErrorsInBackground(conn)
	ntimer := time.NewTimer(HANDLE_REQUEST_TIMEOUT)
	ntimer.Stop()
	timerset := false

	for {
		select {

		case <-conn.server.quitC:
			if !timerset {
				timerset = true
				logger.Warning("startClientAndServerInfiniteCommunicationLoop", fmt.Sprintf("TCP Server ask client %s to timeout operation", conn.ipAddress))
				ntimer.Reset(HANDLE_REQUEST_TIMEOUT)
			}
		case <-ntimer.C:
			panic(fmt.Errorf("TCP client  %s connection timeout", conn.ipAddress))
		case <-conn.quitC:
			return

		case <-processC:
			if conn.server.shuttingDown {
				return
			}

			if !conn.connected {
				return
			}
			logger.Information("startClientAndServerInfiniteCommunicationLoop", fmt.Sprintf("process  TCP client %s request in background", conn.ipAddress))
			processC = processClientRequestAndItsErrorsInBackground(conn)
		default:
			continue
		}
	}
}

func processClientRequestAndItsErrorsInBackground(conn *Connection) <-chan struct{} {

	processC := make(chan struct{})
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				logger.Fatal("processClientRequestAndItsErrorsInBackground", err.(error).Error())
				close(conn.quitC)
				conn.connected = false
			}

			close(processC)
		}()
		//Handle Client request routing.
	}()

	return processC
}

func CreateTcpServer(port int) (*Server, error) {
	address := fmt.Sprintf(":%d", port)
	server, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	watcher, errWatcher := watcher.New(address, processServerQueueOperations)

	if errWatcher != nil {
		return nil, errWatcher
	}

	return &Server{quitC: make(chan struct{}),
		listener: server, watcher: watcher,
		acceptC:     make(chan *Connection, MAX_CONNECTION_CAPACITY),
		started:     false,
		created:     true,
		shutdownC:   make(chan struct{}),
		connections: make(map[string]Connection)}, nil
}

func Start(server *Server) error {

	if server == nil {
		return errors.New("TCP server object must not be a nil")

	}
	message := serverEventMessage{server: server, conn: nil}
	channel, err := watcher.Send(server.watcher, cServerStartOperation, message)
	if err != nil {
		return err
	}
	<-channel
	return nil
}

func Stop(server *Server) error {
	if server == nil {
		return errors.New("TCP server object must not be a nil")
	}
	message := serverEventMessage{server: server, conn: nil}
	channel, err := watcher.Send(server.watcher, cServerShutDownOperation, message)
	if err != nil {
		return err
	}
	logger.Information("Stop", "Waiting for TCP Server to recieve shutdown message...")
	<-channel
	logger.Information("Stop", "TCP Server message loop as be successfully shutdown...")
	return nil
}
