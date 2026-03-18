package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64
type CommonClientAttributes struct {
	ClientId  uint64
	RequestId uint64
}
type CommonKVCommandsAttributes struct {
	CommonClientAttributes
	Key string
}

func (r CommonClientAttributes) GetClientId() uint64 {
	return r.ClientId
}

func (r CommonClientAttributes) GetRequestId() uint64 {
	return r.RequestId
}

func (r CommonKVCommandsAttributes) GetKey() string {
	return r.Key
}

type CommonClientCommandsInterface interface {
	GetClientId() uint64
	GetRequestId() uint64
}

type CommonKVCommandsInterface interface {
	GetClientId() uint64
	GetRequestId() uint64
	GetKey() string
}
type PutArgs struct {
	CommonKVCommandsAttributes
	Value   string
	Version Tversion
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	CommonKVCommandsAttributes
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}
