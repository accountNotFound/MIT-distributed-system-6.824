package raft

import (
	"fmt"
	"os"
	"math/rand"
	"time"
	"sort"
)


func dout(format string, a ...interface{}){
	var debuging=1
	if debuging>0{
		fmt.Printf(format+"\n", a...)		
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

func minAndMediate(arr []int) (int, int){
	var cp=make([]int, len(arr))
	copy(cp, arr)
	sort.Sort(sort.IntSlice(cp))
	return cp[0], cp[len(cp)/2]
}


type tEntry struct{
	Term		int	
	Command		interface{}
}

type tLog struct{
	lastPersist	int
	entries		[]tEntry
}
func makeLog() *tLog{
	var l=tLog{
		lastPersist:	0,
		entries:	[]tEntry{{0, nil}},
	}
	return &l
}
func (l *tLog) size() int{
	return len(l.entries)+l.lastPersist
}
func (l *tLog) back() tEntry{
	return l.entries[len(l.entries)-1]
}
func (l *tLog) get(index int) tEntry{
	if index<l.lastPersist{
		dout("get index(%d)<=lastPersist(%d)", index, l.lastPersist)
		os.Exit(1)
	}
	return l.entries[index-l.lastPersist]
}
func (l *tLog) put(e tEntry){
	l.entries=append(l.entries, e)
}
func (l *tLog) copyRange(begin, end int) []tEntry{
	if begin<l.lastPersist || end<l.lastPersist{
		dout("copy range(%d, %d), lastPersist %d", begin, end, l.lastPersist)
		os.Exit(1)
	}
	var res=make([]tEntry, end-begin)
	copy(res, l.entries[begin-l.lastPersist: end-l.lastPersist])
	return res
}
func (l *tLog) replaceFrom(begin int, arr []tEntry){
	if begin<=l.lastPersist{
		dout("replace from index(%d)<=lastPersist(%d)", begin, l.lastPersist)
		os.Exit(1)
	}
	l.entries=l.entries[0:begin-l.lastPersist]
	l.entries=append(l.entries, arr...)
}
func (l *tLog) compressUntil(end int){
	if end<=l.lastPersist+1{ return }
	var arr=[]tEntry{}
	arr=append(arr, l.entries[end-l.lastPersist-1: l.size()-l.lastPersist]...)
	l.lastPersist=end-1	
	l.entries=arr
}
