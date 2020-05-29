package mr

import "fmt"

type tQueue struct{
	cap		int
	que		chan int
}
func newQueue(capacity int) *tQueue{
	var q=tQueue{
		cap:	capacity,
		que:	make(chan int, capacity),
	}
	return &q
}
func (q *tQueue) size() int{
	return len(q.que)
}
func (q *tQueue) put(v int){
	q.que<-v
}
func (q *tQueue) get() int{
	return <-q.que
}

type tSet struct{
	cap		int
	set		map[int] bool
}
func newSet(capacity int) *tSet{
	var s=tSet{
		cap:	capacity,
		set:	make(map[int] bool),
	}
	return &s
}
func (s *tSet) size() int{
	return len(s.set)
}
func (s *tSet) put(v int){
	s.set[v]=true
}
func (s *tSet) del(v int){
	delete(s.set, v)
}
func (s *tSet) has(v int) bool{
	var _, ok=s.set[v]
	return ok
}

func log(format string, a ...interface{}){
	fmt.Printf(format+"\n", a...)
}