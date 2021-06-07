package main

import (
	"gpmd"
	"log"
	"net"
	"sync"
	"time"
)

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}
func startServer(addr chan string) {
	var foo Foo
	if err := gpmd.Register(&foo); err != nil {
		log.Fatalln("register err:", err)
	}
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalln("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	gpmd.Accept(l)
}

func main() {
	log.SetFlags(0)
	addr := make(chan string)
	go startServer(addr)
	//下面的代码模拟了一个客户端
	client, _ := gpmd.Dial("tcp", <-addr)
	defer func() { _ = client.Close() }()
	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{Num1: i, Num2: i * i}
			var reply int
			if err := client.Call("Foo.Sum", args, &reply); err != nil {
				log.Fatalln("call Foo.Sum error:", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}
