package gpmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"gpmd/codec"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Call struct {
	Seq          uint64      //访问序列号
	ServerMethod string      //调用的方法，像是："<service>.<method>"
	Args         interface{} //调用方法的入参
	Reply        interface{} //调用方法的返回值
	Error        error       //如果出错，记录错误信息
	Done         chan *Call  //调用结束信号(为了支持异步调用)
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec      //cc 是消息的编解码器，和服务端类似，用来序列化将要发送出去的请求，以及反序列化接收到的响应
	opt      *Option          //opt 编解码方式
	sending  sync.Mutex       //sending 是一个互斥锁，和服务端类似，为了保证请求的有序发送，即防止出现多个请求报文混淆
	header   codec.Header     // header 是每个请求的消息头，header 只有在请求发送时才需要，而请求发送是互斥的，因此每个客户端只需要一个，声明在 Client 结构体中可以复用
	mu       sync.Mutex       //mu 互斥锁为了保证client的操作是线程安全的
	seq      uint64           //seq 用于给发送的请求编号，每个请求拥有唯一编号
	pending  map[uint64]*Call //pending 存储未处理完的请求，键是编号，值是 Call 实例
	closing  bool             //closing 和 shutdown 任意一个值置为 true，则表示 Client 处于不可用的状态，但有些许的差别，closing 是用户主动关闭的，即调用 Close 方法，而 shutdown 置为 true 一般是有错误发生
	shutdown bool             //shutdown 链接关闭
}

var _ io.Closer = (*Client)(nil)
var ErrShutdown = errors.New("connection is shut down")

// Close 关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// IsAvailable 返回当前客户端是否可用
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			//通常来说，call为空表示写数据失败，并且call已经被移除
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body failed:" + err.Error())
			}
			call.done()
		}
	}
	//出错了。关闭所有请求
	client.terminateCalls(err)
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodeType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodeType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error", err)
		_ = conn.Close()
		return nil, err
	}
	return NewClientCodec(f(conn), opt), nil
}

func NewClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1,
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

//通过 ...*Option 将 Option 实现为可选参数。
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) > 1 {
		return nil, errors.New("number of options is more than one")
	}

	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodeType == "" {
		opt.CodeType = DefaultOption.CodeType
	}
	return opt, nil
}

// Dial 与服务器建立链接
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

func (client *Client) send(call *Call) {
	//加锁，保证client发送完整的数据
	client.sending.Lock()
	defer client.sending.Unlock()
	//注册一个call
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}
	//封装请求头
	client.header.ServiceMethod = call.ServerMethod
	client.header.Seq = seq
	client.header.Error = ""

	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 实现异步调用
func (client *Client) Go(serverMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 1)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServerMethod: serverMethod,
		Args:         args,
		Reply:        reply,
		Done:         done,
	}
	client.send(call)
	return call
}

//Call 同步调用
func (client *Client) Call(serverMethod string, args, reply interface{}) error {
	call := <-client.Go(serverMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}

type clientResult struct {
	client *Client
	err    error
}
type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}
