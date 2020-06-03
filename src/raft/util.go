
package raft

import (
	"fmt"
	"math/rand"
	"time"
	"sort"
	"sync"
)

var mtx sync.Mutex
var _TimeStamp=0

func dout(format string, a ...interface{}){
	if _UseDebug>0{
		mtx.Lock()
		fmt.Printf("%d: ",_TimeStamp)
		fmt.Printf(format+"\n", a...)
		_TimeStamp++
		mtx.Unlock()		
	}

}

func min(a, b int) int{
	if a<b{ return a }
	return b
}
func max(a, b int) int{
	if a>b{ return a }
	return b
}

func randDuration(lower, upper time.Duration) time.Duration {
	num := rand.Int63n(upper.Nanoseconds()-lower.Nanoseconds()) + lower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}

func mediate(arr []int) int{
	var cp=make([]int, len(arr))
	copy(cp, arr)
	sort.Sort(sort.IntSlice(cp))
	return cp[len(cp)/2]
}


type tEntry struct{
	Term		int	
	Command		interface{}
}

type tEntrySlice []tEntry
func (ls tEntrySlice) copy(begin, end int) tEntrySlice{
	if end<=begin{ return tEntrySlice{} }
	var res=make([]tEntry, end-begin)
	copy(res, ls[begin: end])
	return tEntrySlice(res)
}

type tLog struct{
	offset	int
	slice	[]tEntry
}
func (log *tLog) size() int{
	return log.offset+len(log.slice)
}
func (log *tLog) base() int{
	return log.offset
}
func (log *tLog) get(index int) tEntry{
	return log.slice[index-log.offset]
}
func (log *tLog) last() tEntry{
	var index=len(log.slice)-1
	return log.slice[index-log.offset]
}
func (log *tLog) append(cmd ...tEntry){
	log.slice=append(log.slice, cmd...)
}
func (log *tLog) keep(begin, end int){
	log.slice=log.slice[begin-log.offset: end-log.offset]
}
func (log *tLog) copy(begin, end int) tEntrySlice{
	var res=make([]tEntry, end-begin)
	copy(res, log.slice[begin-log.offset: end-log.offset])
	return tEntrySlice(res)
}