package kvsrv

// Put or Append

type RpcArgs interface {
	GetClientName() string
	GetRequestId() int
}

type RpcReply interface {
	GetVal() string
	SetVal(val string)
}

type PutAppendArgs struct {
	Key   string
	Value string
	
	ClientName string
	RequestId int
}

func (pargs *PutAppendArgs) GetClientName() string {
	return pargs.ClientName
}

func (pargs *PutAppendArgs) GetRequestId() int {
	return pargs.RequestId
}

var _ RpcArgs = &PutAppendArgs{}

type PutAppendReply struct {
	Value string
}

func (preply *PutAppendReply) GetVal() string {
	return preply.Value
}

func (preply *PutAppendReply) SetVal(val string) {
	preply.Value = val
}

var _ RpcReply = &PutAppendReply{}

type GetArgs struct {
	Key string
	
	ClientName string
	RequestId int
}

func (gargs *GetArgs) GetClientName() string {
	return gargs.ClientName
}

func (gargs *GetArgs) GetRequestId() int {
	return gargs.RequestId
}

var _ RpcArgs = &GetArgs{}

type GetReply struct {
	Value string
}

func (greply *GetReply) GetVal() string {
	return greply.Value
}

func (greply *GetReply) SetVal(val string) {
	greply.Value = val
}

var _ RpcReply = &GetReply{}