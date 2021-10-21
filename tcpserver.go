package tcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"time"
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
	watcher      *watchers.Watcher
	shuttingDown bool
	created      bool
	started      bool
	shutdownC    chan struct{}
	quitC        chan struct{}
	acceptC      chan *clientConnection
	connections  map[string]clientConnection
}

type clientConnection struct {
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
	conn       *clientConnection
}

type ServerOperationResult struct {
	conn *clientConnection
}

type TcpRequest struct {
	Method string
	Body   []byte
}

type TcpResponse struct {
	Method string
	Status bool
	Body   []byte
}

func createEndpointController(response io.Writer, request io.Reader) {
	loggers.Debug("TcpCreateEndpointController: method called")
	tcpRequest := TcpRequest{}
	err := json.NewDecoder(request).Decode(&tcpRequest)
	if err != nil {
		panic(err)
	}

	data := stores.CreateEndpointRequest{}
	reader := bytes.NewReader(tcpRequest.Body)

	err = json.NewDecoder(reader).Decode(&data)

	if err != nil {
		panic(err)
	}
	result := stores.CreateNodeEntryAccess(data)

	tcpResponse := TcpResponse{}
	tcpResponse.Status = true
	tcpResponse.Body, _ = json.Marshal(result)
	tcpResponse.Method = tcpRequest.Method
	json.NewEncoder(response).Encode(tcpResponse)
	loggers.Debug(string(tcpResponse.Body))

}

func updateStorageEndpointController(response io.Writer, request io.Reader) {

	tcpRequest := TcpRequest{}
	err := json.NewDecoder(request).Decode(&tcpRequest)
	if err != nil {
		panic(err)
	}
	updateRequest := stores.UpdateStorageEndpointRequest{}
	json.Unmarshal(tcpRequest.Body, &updateRequest)

	result := stores.UpdateNodeEntryAccess(updateRequest)

	tcpResponse := TcpResponse{}
	tcpResponse.Status = true
	reponseBytes, errReponse := json.Marshal(result)
	tcpResponse.Body = reponseBytes
	if errReponse != nil {
		panic(errReponse)
	}
	json.NewEncoder(response).Encode(tcpResponse)

}

func removeEndpointFromStorageController(response io.Writer, request io.Reader) {
	tcpRequest := TcpRequest{}
	json.NewDecoder(request).Decode(&tcpRequest)

	removeContentRequest := stores.DeleteEndpointStorageRequest{}
	err := json.Unmarshal(tcpRequest.Body, &removeContentRequest)
	if err != nil {
		panic(err)
	}

	result := stores.DeleteNodeEntryAccess(removeContentRequest)
	//send the response back to the user
	tcpResponse := TcpResponse{}
	tcpResponse.Status = true
	tcpResponse.Method = tcpRequest.Method
	tcpResponse.Body, _ = json.Marshal(result)
	json.NewEncoder(response).Encode(tcpResponse)

}

func handleBadRequestErrorController(response io.Writer, request io.Reader) {
	requestObject := TcpRequest{}
	err := json.NewDecoder(request).Decode(&requestObject)
	if err != nil {
		panic(err)
	}
	httpError := stores.ErrorMessageType{ErrorMessage: "Bad TCP request or resource not found."}

	results := TcpResponse{}
	results.Status = true
	results.Body, _ = json.Marshal(httpError)
	results.Method = requestObject.Method
	json.NewEncoder(response).Encode(results)
}

func getStorageEndpointController(response io.Writer, request io.Reader) {
	tcpRequest := TcpRequest{}
	err := json.NewDecoder(request).Decode(&tcpRequest)
	if err != nil {
		panic(err)
	}

	data := stores.GetStorageEndPointRequest{}
	err = json.Unmarshal(tcpRequest.Body, &data)
	if err != nil {
		panic(err)
	}

	storage := stores.GetOrCreateIfNotExistsStoreInstance(stores.KVS_NAME)

	getResult := stores.Get(storage, data.Key)

	result := stores.GetStorageEndpointResult{}
	result.Success = getResult.Success
	result.ErrorMessage = getResult.ErrorMessage

	if result.Success {
		result.Endpoint = getResult.Item.(stores.NodeEntry)
	}

	tcpResponse := TcpResponse{Status: true}
	tcpResponse.Body, _ = json.Marshal(result)
	tcpResponse.Method = tcpRequest.Method
	json.NewEncoder(response).Encode(tcpResponse)
}

func getAllStorageEndPointController(response io.Writer, request io.Reader) {

	result := stores.GetStoreItemResult{Endpoints: make(map[string]stores.NodeEntry), Counts: 0}

	storage := stores.GetOrCreateIfNotExistsStoreInstance(stores.KVS_NAME)

	resultChannel := stores.All(storage)

	for storeResult := range resultChannel {

		result.Counts++
		result.ErrorMessage = storeResult.ErrorMessage
		result.Endpoints[storeResult.Key] = storeResult.Item.(stores.NodeEntry)
	}

	tcpResponse := TcpResponse{Status: true}
	data, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	tcpResponse.Body = data
	json.NewEncoder(response).Encode(tcpResponse)
}

func createServerConnection(server *Server, conn net.Conn) *clientConnection {
	message := serverEventMessage{server: server, nativeConn: conn}
	channel, err := watchers.SendOperation(server.watcher, cCreateServerConnectionOperation, message)
	if err != nil {
		loggers.Warning(err.Error())
	}
	result := (<-channel).(ServerOperationResult)
	return result.conn
}

func notifyServerClientMessageLoopTerminated(server *Server, conn *clientConnection) {
	message := serverEventMessage{server: server, conn: conn}
	channel, err := watchers.SendOperation(server.watcher, cRemoveAndShutdownClientConnectionOperation, message)
	if err != nil {
		loggers.Warning(err.Error())
		return
	}
	<-channel
}

func tellServerClientLoopHasBeenTerminated(server *Server) {
	message := serverEventMessage{server: server, conn: nil}
	channel, err := watchers.SendOperation(server.watcher, cServerLoopShutDownOperation, message)
	if err != nil {
		loggers.Warning(err.Error())
		return
	}
	<-channel
}

func processServerQueueOperations(op watchers.WatcherOperation) {

	message := op.Item.(serverEventMessage)
	result := ServerOperationResult{}

	switch op.Type {

	case cRemoveAndShutdownClientConnectionOperation:

		if message.conn != nil {
			loggers.Information(fmt.Sprintf("TCP Server terminating client %s ", message.conn.ipAddress))

			if len(message.server.connections) > 0 {
				delete(message.server.connections, message.conn.id)
			}
			message.conn.client.Close()
			loggers.Debug(fmt.Sprintf("TCP client connection %s as be closed ", message.conn.ipAddress))
		}

		if message.server.shuttingDown {

			if len(message.server.connections) == 0 {
				watchers.Stop(message.server.watcher)
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
			loggers.Information(fmt.Sprintf("adding new TCP connection %s", message.nativeConn.LocalAddr()))
			acceptedDateTime := time.Now()
			var index int64 = int64(len(message.server.connections))
			client := clientConnection{client: message.nativeConn,
				server:             message.server,
				connectedTimestamp: acceptedDateTime}
			client.ipAddress = message.nativeConn.RemoteAddr().String()
			client.connected = true
			client.quitC = make(chan struct{})
			client.id = strconv.FormatInt(index, 10)
			message.server.connections[client.id] = client
			result.conn = &client
			loggers.Debug(fmt.Sprintf("Total TCP connection(s) in processing queue %d", len(message.server.connections)))
		}
		op.Channel <- result

	case cServerStartOperation:
		if message.server.started {
			loggers.Information(fmt.Sprintf("TCP Server started  already  at : %s", message.server.listener.Addr()))
			return
		}
		loggers.Information(fmt.Sprintf("Starting  TCP Server at : %s", message.server.listener.Addr()))
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
				loggers.Error(err.(error))
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
				loggers.Error(err.(error))
			}
			close(acceptedC)
		}()

		loggers.Debug("Waiting to accept  TCP connection...")
		conn, err := server.listener.Accept()
		if err != nil {
			loggers.Error(err)
			return
		}

		if server.shuttingDown {
			loggers.Error(errors.New("@acceptConnection: TCP server is shutting down"))
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
			loggers.Information(fmt.Sprintf("TCP connection channel recieved from %s ", c.ipAddress))
			go startClientAndServerInfiniteCommunicationLoop(c)

		}
	}()
}

func startClientAndServerInfiniteCommunicationLoop(conn *clientConnection) {

	defer func() {
		err := recover()
		if err != nil {
			loggers.Error(err.(error))
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
				loggers.Warning(fmt.Sprintf("TCP Server ask client %s to timeout operation", conn.ipAddress))
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
			loggers.Information(fmt.Sprintf("process  TCP client %s request in background", conn.ipAddress))
			processC = processClientRequestAndItsErrorsInBackground(conn)
		default:
			continue
		}
	}
}

func processClientRequestAndItsErrorsInBackground(conn *clientConnection) <-chan struct{} {

	processC := make(chan struct{})
	go func() {

		defer func() {
			err := recover()
			if err != nil {
				loggers.Warning(err.(error).Error())
				close(conn.quitC)
				conn.connected = false
			}

			close(processC)
		}()

		loggers.Debug(fmt.Sprintf("Waiting for TCP client %s to send its request", conn.ipAddress))
		request := TcpRequest{}
		err := json.NewDecoder(conn.client).Decode(&request)

		if err != nil {
			var errorMessage error
			if err == io.EOF {
				errorMessage = fmt.Errorf("TCP client endpoint (%s) has terminated, from the remote side", conn.ipAddress)
			} else {
				errorMessage = fmt.Errorf("%s: from TCP client %s ", err.Error(), conn.ipAddress)
			}
			panic(errorMessage)
		}

		reqbytes, _ := json.Marshal(request)
		reader := bytes.NewReader(reqbytes)

		switch request.Method {
		case cCreateKvsItemRequest:
			createEndpointController(conn.client, reader)
		case cDeleteKvsItemRequest:
			removeEndpointFromStorageController(conn.client, reader)
		case cGetAllKvsItemRequest:
			getAllStorageEndPointController(conn.client, reader)
		case cGetKvsItemRequest:
			getStorageEndpointController(conn.client, reader)
		case cUpdateKvsItemRequest:
			updateStorageEndpointController(conn.client, reader)
		default:
			handleBadRequestErrorController(conn.client, reader)
		}

		loggers.Debug(fmt.Sprintf("TCP client endpoint (%s) request completed without error", conn.ipAddress))
	}()

	return processC
}

func CreateTcpServer(port int) (*Server, error) {
	address := fmt.Sprintf(":%d", port)
	server, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	watcher, errWatcher := watchers.CreateWatcher(address, processServerQueueOperations)

	if errWatcher != nil {
		return nil, errWatcher
	}

	return &Server{quitC: make(chan struct{}),
		listener: server, watcher: watcher,
		acceptC:     make(chan *clientConnection, MAX_CONNECTION_CAPACITY),
		started:     false,
		created:     true,
		shutdownC:   make(chan struct{}),
		connections: make(map[string]clientConnection)}, nil
}

func Start(server *Server) error {

	if server == nil {
		return errors.New("TCP server object must not be a nil")

	}
	message := serverEventMessage{server: server, conn: nil}
	channel, err := watchers.SendOperation(server.watcher, cServerStartOperation, message)
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
	channel, err := watchers.SendOperation(server.watcher, cServerShutDownOperation, message)
	if err != nil {
		return err
	}
	loggers.Information("Waiting for TCP Server to recieve shutdown message...")
	<-channel
	loggers.Information("TCP Server message loop as be successfully shutdown...")
	return nil
}
