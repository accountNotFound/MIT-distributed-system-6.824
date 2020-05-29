package mr

import "testing"
import "time"
import "net"
import "net/http"
import "net/rpc"
import "fmt"

func TestBasic(t *testing.T){
	log("TestBasic begin")
	var files=[]string{"abc", "rst", "xyz"}
	var m=MakeMaster(files, 5)
	for i:=0;i<8;i++{
		go Worker()
	}
	for !m.Done(){
		time.Sleep(time.Second)
	}
	log("TestBasic end")
}



// example of how to RPC below
type Server struct{}
func (s *Server)Inc(req *int, res *int) error{
	*res=*req+1
	return nil
}

type Client struct{}

func TestRPCExample(t *testing.T){
	rpc.Register(new(Server))
	rpc.HandleHTTP()
	sock, err:=net.Listen("tcp", "127.0.0.1:1234")
	if err!=nil{
		log("fatal: master net listen err %v", err)
		return
	}
	go http.Serve(sock, nil)

	conn, err:=rpc.DialHTTP("tcp", "127.0.0.1:1234")
	if err!=nil{
		log("fatal: connect to master err %v", err)
		return
	}
	var req=1
	var res=100
	conn.Call("Server.Inc", &req, nil)
	fmt.Println(res)
	time.Sleep(5)
	fmt.Println("test end")
}