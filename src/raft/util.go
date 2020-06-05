
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
func minimum(arr []int) int{
	var res=arr[0]
	for i:=range arr{ res=min(res, arr[i]) }
	return res
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
func (log *tLog) get(index int) tEntry{
	return log.slice[index-log.offset]
}
func (log *tLog) last() tEntry{
	return log.slice[len(log.slice)-1]
}
func (log *tLog) append(cmd ...tEntry){
	log.slice=append(log.slice, cmd...)
}
func (log *tLog) keep(begin, end int){
	begin=max(begin, log.offset)
	end=min(end, log.size())
	log.slice=log.slice[begin-log.offset: end-log.offset]
}
func (log *tLog) copy(begin, end int) tEntrySlice{
	var res=make([]tEntry, end-begin)
	begin=max(begin, log.offset)
	end=min(end, log.size())	
	copy(res, log.slice[begin-log.offset: end-log.offset])
	return tEntrySlice(res)
}