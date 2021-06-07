package gpmd

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

//methodType 实例包含了一个方法的完整信息
type methodType struct {
	method    reflect.Method //方法本身
	ArgType   reflect.Type   //第一个参数的类型
	ReplyType reflect.Type   //第二个参数的类型
	numCalls  uint64         //统计调用次数
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *methodType) newReply() reflect.Value {
	replyValue := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyValue.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyValue.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyValue
}

type service struct {
	name   string                 // name 映射结构体名字
	typ    reflect.Type           // typ 结构体类型
	rcvr   reflect.Value          // rcvr 结构体的实例本身，保留 rcvr 是因为在调用时需要 rcvr 作为第 0 个参数
	method map[string]*methodType //method 是 map 类型，存储映射的结构体的所有符合条件的方法
}

func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server:%s is not a valid service name", s.name)
	}
	s.registerMethod()
	return s
}

//registerMethod 过滤出复合RPC调用规则的方法
//两个导出或内置类型的入参（反射时为 3 个，第 0 个是自身，类似于 python 的 self，java 中的 this）
//返回值有且只有 1 个，类型为 error
func (s *service) registerMethod() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc service: register %s.%s", s.name, method.Name)
	}
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

//call 方法，即能够通过反射值调用方法
func (s *service) call(m *methodType, argv, replayValue reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replayValue})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}
