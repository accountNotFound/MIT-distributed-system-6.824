package kvraft

import "fmt"

type _OpType uint8
const (
	_GET	 _OpType=iota
	_PUT
	_APPEND
)
func (t _OpType) String() string{
	switch t{
	case _GET: return "Get"
	case _PUT: return "Put"
	case _APPEND:   return "Append"
	default:		return ""
	}
}


// Args for all operations
type Args struct{
	UID		int
	SEQ		int
	Type	_OpType
	Key		string
	Value	string	
}
func (a *Args) String() string{
	var value=a.Value
	if len(value)>5{ value=value[0: 5] }
	return fmt.Sprintf("{%v %v %v \"%s\" \"%s\"}", a.UID, a.SEQ, a.Type, a.Key, value)
}


// Reply for all operations
type Reply struct{
	// UID			int
	Valid		bool	// whether this reply.Value is valid
	Value		string
}
func (r *Reply) String() string{
	var value=r.Value
	if len(value)>5{ value=value[0: 5]+"..." }
	return fmt.Sprintf("{%v \"%s\"}", r.Valid, value)
}