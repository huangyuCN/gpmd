package gpmd

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

func startServer(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalln("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	Accept(l)
}

func TestSyncCall(t *testing.T) {
	log.SetFlags(0)
	addr := make(chan string)
	go startServer(addr)
	client, _ := Dial("tcp", <-addr)
	defer func() { _ = client.Close() }()
	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer func() {
				wg.Done()
				cancel()
			}()
			args := fmt.Sprintf("gpmd req %d", i)
			var reply string
			if err := client.Call(ctx, "Foo.Sum", args, &reply); err != nil {
				log.Fatalln("call Foo.Sum error:", err)
			}
			log.Println("reply:", reply)
		}(i)
	}
	wg.Wait()
}
