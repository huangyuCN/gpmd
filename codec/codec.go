package codec

import "io"

type Header struct {
	ServiceMethod string //解析"Service.Method"，通常与 Go 语言中的结构体和方法相映射
	Seq           uint64 //客户端提供的标志某一次请求的序列号
	Error         string //错误信息，客户端置为空，服务端如果如果发生错误，将错误信息置于 Error 中
}

//Codec 抽象出对消息体进行编解码的接口 Codec，抽象出接口是为了实现不同的 Codec 实例
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

//NewCodecFunc 是Codec的构造函数
type NewCodecFunc func(closer io.ReadWriteCloser) Codec

type Type string

//定义了 2 种 Codec，Gob 和 Json，但是实际代码中只实现了 Gob 一种，
//事实上，2 者的实现非常接近，甚至只需要把 gob 换成 json 即可。
const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json" //本项目中未实现
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
