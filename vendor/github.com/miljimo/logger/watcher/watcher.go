package watcher

/*
 */

import (
	"errors"
	"fmt"
)

const (
	cStopOperationType = "cStopOperationType"
	cUserDefined       = "cUserDefinedOperationType"
)

var (
	nullWatchError error
	handlerError   error
	createNewError error
)

func init() {
	nullWatchError = errors.New("watcher cannot be null")
	handlerError = errors.New("watcher must be create with the CreateWatcher to work")
	createNewError = errors.New("unable to create a watcher without a name")

}

type Operation struct {
	Channel chan interface{}
	Item    interface{}
	Type    string
	Error   error
}

// MessageOperationFunc the callback function to handle  synchronisation of channels.
type OperationHandler func(operation Operation)

// Watcher the main object type
type Watcher struct {
	name     string
	channel  chan Operation
	handler  OperationHandler
	shutdown bool
}

//handleWatcherOperation default message loop
func handleWatcherOperation(wc *Watcher) {
	for operation := range wc.channel {
		if wc.shutdown {
			return
		}
		switch operation.Type {

		case cStopOperationType:
			wc.shutdown = true
			close(wc.channel)
			if len(wc.channel) > 0 {
				processingRemainingChannelOperation(wc)
			}
			fmt.Printf("Watcher (%v) operation loop closed\n", wc.name)
			close(operation.Channel)
			return
		default:
			wc.handler(operation)
		}
	}
}

//processingRemainingChannelOperation the rest of the channel data
func processingRemainingChannelOperation(wc *Watcher) {
	for innerOp := range wc.channel {
		if innerOp.Type != cStopOperationType {
			close(innerOp.Channel)
			wc.handler(innerOp)
		}
	}
}

// New create a new Watcher object for synchronisation with channel
func New(name string, handler OperationHandler) (*Watcher, error) {
	if name == "" {
		return nil, createNewError
	}
	if handler == nil {
		return nil, handlerError
	}
	//create some kind of rule to create a channel watcher
	wc := &Watcher{name: name,
		channel:  make(chan Operation, 1),
		handler:  handler,
		shutdown: false}

	// create a new go routine to handler the operations
	go func(watcher *Watcher) {
		defer func() {
			err := recover()
			if err != nil {
				fmt.Println("@CreateWatcher.handleWatcherOperation, Error:", err)
			}
		}()
		handleWatcherOperation(watcher)
	}(wc)
	return wc, nil
}

//Customer operation type
func Send(watcher *Watcher, oType string, item interface{}) (<-chan interface{}, error) {
	if (watcher == nil) ||
		(watcher.handler == nil) {
		return nil, nullWatchError
	}
	// dont wait fro read , just read since all channel blocks during reading
	op := Operation{Item: item, Type: oType, Channel: make(chan interface{}, 1)}
	watcher.channel <- op
	return op.Channel, nil
}

func Stop(watcher *Watcher) error {
	channel, err := Send(watcher, cStopOperationType, struct{}{})
	if err != nil {
		return err
	}
	<-channel
	return err
}
