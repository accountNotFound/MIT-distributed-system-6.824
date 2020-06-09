
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

func dump(format string, a ...interface{}){
	if useDebug>0{
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
