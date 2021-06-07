package main

import (
	"context"
	"gpmd"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

//---------------------TCP test---------------------------
//type Foo int
//
//type Args struct{ Num1, Num2 int }
//
//func (f Foo) Sum(args Args, reply *int) error {
//	*reply = args.Num1 + args.Num2
//	return nil
//}
//func startServer(addr chan string) {
//	var foo Foo
//	if err := gpmd.Register(&foo); err != nil {
//		log.Fatalln("register err:", err)
//	}
//	l, err := net.Listen("tcp", ":0")
//	if err != nil {
//		log.Fatalln("network error:", err)
//	}
//	log.Println("start rpc server on", l.Addr())
//	addr <- l.Addr().String()
//	gpmd.Accept(l)
//}
//
//func main() {
//	log.SetFlags(0)
//	addr := make(chan string)
//	go startServer(addr)
//	//下面的代码模拟了一个客户端
//	client, _ := gpmd.Dial("tcp", <-addr)
//	defer func() { _ = client.Close() }()
//	time.Sleep(time.Second)
//	var wg sync.WaitGroup
//	for i := 0; i < 5; i++ {
//		wg.Add(1)
//		go func(i int) {
//			//ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
//			defer func() {
//				wg.Done()
//				//cancel()
//			}()
//			args := &Args{Num1: i, Num2: i * i}
//			var reply int
//			if err := client.Call(context.Background(), "Foo.Sum", args, &reply); err != nil {
//				log.Fatalln("call Foo.Sum error:", err)
//			}
//			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
//		}(i)
//	}
//	wg.Wait()
//}

// -----------------------HTTP TEST----------------------
type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}
func startServer(addr chan string) {
	var foo Foo
	l, _ := net.Listen("tcp", ":9999")
	_ = gpmd.Register(&foo)
	gpmd.HandleHTTP()
	addr <- l.Addr().String()
	_ = http.Serve(l, nil)
}

func call(addrCh chan string) {
	addr := <-addrCh
	client, _ := gpmd.DialHTTP("tcp", addr)
	defer func() {
		_ = client.Close()
	}()
	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			context, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer func() {
				wg.Done()
				cancel()
			}()
			args := &Args{
				Num1: i,
				Num2: i * i,
			}
			var reply int
			if err := client.Call(context, "Foo.Sum", args, &reply); err != nil {
				log.Fatal("call Foo.Sum error:", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
		wg.Wait()
	}
}

func main() {
	log.SetFlags(0)
	ch := make(chan string)
	go call(ch)
	startServer(ch)
}
