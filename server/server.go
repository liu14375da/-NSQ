package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"sync"
	"time"
)

//声明一个结构体，实现HandleMessage接口方法（根据文档的要求）
type NsqHandler struct {
	//消息数
	msqCount int64
	//标识ID
	nsqHandlerID string
}


var (
	//nsqd的地址，使用了tcp监听的端口
	tcpNsqdAddrr = "120.79.0.106:4150"
)

func (n NsqHandler) HandleMessage(message *nsq.Message) error {
	n.msqCount++
	fmt.Println(n.msqCount)
	fmt.Printf("msg.Timestamp=%v, msg.nsqaddress=%s,msg.body=%s \n",
		time.Unix(0,message.Timestamp).Format("2006-01-02 03:04:05"),
			message.NSQDAddress,string(message.Body))
	return nil
}

func main() {
	//初始化配置
	config:=nsq.NewConfig()
	//创造消费者，参数一时订阅的主题，参数二是使用的通道
	com,err:=nsq.NewConsumer("Insert","chanell",config)
	if err != nil {
		fmt.Println(err)
	}
	//添加处理回调
	com.AddHandler(&NsqHandler{nsqHandlerID: "One"})
	//连接对应的nsqd
	err=com.ConnectToNSQD(tcpNsqdAddrr)
	if err != nil {
		fmt.Println(err)
	}
	//只是为了不结束此进程，这里没有意义
	var wg=&sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
