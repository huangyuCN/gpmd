package gpmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"gpmd/codec"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0x1234567

type Option struct {
	MagicNumber    int           //MagicNumber 用来标志这是一个gpmd请求，类似erlang的session key
	CodeType       codec.Type    //客户端使用的用来编码body的方式
	ConnectTimeout time.Duration //Client.Call 链接超时
	HandleTimeout  time.Duration //server.handleRequest 处理超时
}

//DefaultOption 一般来说，涉及协议协商的这部分信息，需要设计固定的字节来传输的。
//但是为了实现上更简单，GeeRPC 客户端固定采用 JSON 编码 Option，后续的 header
//和 body 的编码方式由 Option 中的 CodeType 指定，服务端首先使用 JSON 解码 Option，
//然后通过 Option 的 CodeType 解码剩余的内容。即报文将以这样的形式发送
//| Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
//| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
//
//在一次连接中，Option 固定在报文的最开始，Header 和 Body 可以有多个，即报文可能是这样的:
//| Option | Header1 | Body1 | Header2 | Body2 | ...
var DefaultOption = &Option{
	MagicNumber,
	codec.GobType,
	10 * time.Second, //ConnectTimeout 默认值为 10s
	0,                //HandleTimeout 默认值为 0，即不设限
}

type Server struct {
	serviceMap sync.Map
}

var DefaultServer = NewServer()

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go s.ServeConn(conn)
	}
}

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error:", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server:invalid codec type %s\n", opt.CodeType)
		return
	}
	f := codec.NewCodecFuncMap[opt.CodeType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodeType)
		return
	}
	s.serveCodec(f(conn), &opt)
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

func (s *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex) //确保发送完整的response
	wg := new(sync.WaitGroup)  //确保所有的请求都被处理完
	for {
		req, err := s.readRequest(cc)
		if err != nil {
			if req == nil {
				break //出错了，关闭连接
			}
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

//request 保存一次请求的所有信息
type request struct {
	h            *codec.Header //请求中的header信息
	argv, replyv reflect.Value //argv and replyv of request
	mType        *methodType
	svc          *service
}

func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

//readRequest 方法中最重要的部分，即通过 newArgv() 和 newReplyv()
//两个方法创建出两个入参实例，然后通过 cc.ReadBody() 将请求报文反序列化为
//第一个入参 argv，在这里同样需要注意 argv 可能是值类型，也可能是指针类型，所以处理方式有点差异。
func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := s.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mType, err = s.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mType.newArgv()
	req.replyv = req.mType.newReply()
	argvInterface := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvInterface = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvInterface); err != nil {
		log.Println("rpc server: read argv error:", err)
		return req, err
	}
	return req, nil
}

func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	//这里需要确保 sendResponse 仅调用一次，因此将整个过程拆分为 called 和 sent 两个阶段
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mType, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
	}()
	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		s.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

func (s *Server) Register(rcvr interface{}) error {
	service := newService(rcvr)
	if _, dup := s.serviceMap.LoadOrStore(service.name, service); dup {
		return errors.New("rpc: service already defined:" + service.name)
	}
	return nil
}

func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

//findService 的实现看似比较繁琐，但是逻辑还是非常清晰的。
//因为 ServiceMethod 的构成是 “Service.Method”，因此先将其分割成 2 部分，
//第一部分是 Service 的名称，第二部分即方法名。现在 serviceMap 中找到对应的 service 实例，
//再从 service 实例的 method 中，找到对应的 methodType
func (s *Server) findService(serviceMethod string) (svc *service, mType *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed:" + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	serviceInterface, ok := s.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service" + serviceName)
		return
	}
	svc = serviceInterface.(*service)
	mType = svc.method[methodName]
	if mType == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}
