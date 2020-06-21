package kvraft

import "fmt"

type _OpType uint8
const (
	_GET	_OpType=iota
	_PUT
	_APPEND
)
func (t _OpType) String() string{
	switch t{
	case _GET: return "Get"
	case _PUT: return "Put"
	default:   return "Append"
	}
}

// Meta is public for RPC
type Meta struct{
	ClientID	int64
	RequestID	int
}

// Args for all operations
type Args struct{
	Meta
	Type	_OpType
	Key		string
	Value	string	
}
func (a *Args) String() string{
	var value=a.Value
	// if len(value)>5{ value=value[0: 5] }
	return fmt.Sprintf("{%v %v \"%s\" \"%s\"}", a.Meta, a.Type, a.Key, value)
}


// Reply for all operations
type Reply struct{
	Meta
	Redirect	bool
	LeaderID	int
	Value		string
}
func (r *Reply) String() string{
	var value=r.Value
	// if len(value)>5{ value=value[0: 5] }
	return fmt.Sprintf("{%v %v %v \"%s\"}", r.Meta, r.Redirect, r.LeaderID, value)
}