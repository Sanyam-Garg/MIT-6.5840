package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClientData struct {
	lastSuccessfulRequestId int
	lastSuccessfulResponse  string
}
type KVServer struct {
	mu sync.Mutex

	// key-value store
	store map[string]string
	// contains the last successful request id and response for a client
	pastRequests map[string]ClientData
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	if kv.canSendPreviousResponse(args, reply) {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientData := ClientData{
		lastSuccessfulRequestId: args.RequestId,
	}

	val, exists := kv.store[args.Key]

	if exists {
		reply.Value = val
		clientData.lastSuccessfulResponse = val
	} else {
		clientData.lastSuccessfulResponse = ""
	}

	kv.pastRequests[args.ClientName] = clientData
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {

	if kv.canSendPreviousResponse(args, reply) {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientData := ClientData{
		lastSuccessfulRequestId: args.RequestId,
		lastSuccessfulResponse: args.Value,
	}

	kv.store[args.Key] = args.Value

	reply.Value = args.Value

	kv.pastRequests[args.ClientName] = clientData
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {

	if kv.canSendPreviousResponse(args, reply) {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	currentVal, exists := kv.store[args.Key]

	clientData := ClientData{
		lastSuccessfulRequestId: args.RequestId,
	}

	if !exists {
		kv.store[args.Key] = args.Value
		clientData.lastSuccessfulResponse = ""
	} else {
		kv.store[args.Key] = currentVal + args.Value
		reply.Value = currentVal
		clientData.lastSuccessfulResponse = currentVal
	}

	kv.pastRequests[args.ClientName] = clientData
}

func (kv *KVServer) canSendPreviousResponse(args RpcArgs, reply RpcReply) bool {
	kv.mu.Lock()
	lastResponse, exists := kv.pastRequests[args.GetClientName()]
	kv.mu.Unlock()
	
	if !exists {
		return false
	}

	if lastResponse.lastSuccessfulRequestId != args.GetRequestId() {
		return false
	}

	reply.SetVal(lastResponse.lastSuccessfulResponse)
	return true
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		mu:    sync.Mutex{},
		store: make(map[string]string),
		pastRequests: make(map[string]ClientData),
	}

	return kv
}
