package main

import (
	"flag"
	"fmt"
	"k8s.io/klog/v2"
	"net"
	"soapa-app/pkg/soapa_util/utils"
	"sync"
	"time"
)

// tcp 代理

const (
	ListendHost = "0.0.0.0"
)

type proxyConfig struct {
	// 监听本地端口 代理到目标地址
	ProxyConfig map[string]string `json:"proxy_config"`

	ReadDeadline  int64 `json:"read_deadline"`
	WriteDeadline int64 `json:"write_deadline"`
	BuffSize      int   `json:"buff_size"`
}

var app_conf proxyConfig
var wg sync.WaitGroup

func runListenr() {
	for k, v := range app_conf.ProxyConfig {
		address := fmt.Sprintf("%s:%s", ListendHost, k)
		wg.Add(1)
		go func(proxyAddress string) {
			listener, err := net.Listen("tcp", address)
			defer wg.Done()
			if err != nil {
				klog.Infof("init listen err %s, adress %v, proxt to %s", err, address, v)
				return
			}
			defer listener.Close()

			klog.Infof("listen tcp started, listen %s ,proxy %s", address, proxyAddress)
			for {
				conn, err := listener.Accept()

				if err != nil {
					klog.Errorf("accept err %s", err)
				} else {
					//	处理请求
					go connectionHadnler(conn, proxyAddress)
				}
			}
		}(v)
	}

	wg.Wait()
}

func connectionHadnler(conn net.Conn, backend string) {
	klog.Infof("conn %s accept, proxy to %s", conn.RemoteAddr().String(), backend)
	target, err := net.Dial("tcp", backend)
	defer conn.Close()

	if err != nil {
		klog.Errorf("connect dial err %s %s", backend, err)
	} else {
		defer target.Close()

		klog.Infof("dial conn %s , proxy to %s connected", conn.RemoteAddr().String(), target.RemoteAddr().String())
		closed := make(chan bool, 2)

		// 双向读写
		go proxy(conn, target, closed)
		go proxy(target, conn, closed)

		//  等待双向都关闭 or 有一个任意关闭？
		<-closed
		klog.Infof("conn remote %s , connection closed", conn.RemoteAddr().String())
	}
}

func proxy(from net.Conn, to net.Conn, closed chan bool) {
	buffer := make([]byte, app_conf.BuffSize)
	for {
		err := from.SetReadDeadline(time.Now().Add(time.Duration(app_conf.ReadDeadline) * time.Second))
		if err != nil {
			klog.Errorf("set read deadline err %s", err)
			closed <- true
			return
		}
		// 读数据
		n1, err := from.Read(buffer)
		if err != nil {
			closed <- true
			return
		}
//fmt.Println(">>>>>>>>>>>....  buff", string(buffer))
		// 写数据
		err = to.SetReadDeadline(time.Now().Add(time.Duration(app_conf.WriteDeadline) * time.Second))
		if err != nil {
			klog.Errorf("set write deadline err %s", err)
			closed <- true
		}
		_, err = to.Write(buffer[:n1])
//klog.Infof("from %s recv %d,  to %s send %d", from.RemoteAddr().String(), n1, to.RemoteAddr().String(), n2)
		if err != nil {
			klog.Infof("to %s write err %s", to.RemoteAddr().String(), err)
			closed <- true
			return
		}
	}
}

func main() {
	var conf string
	var err error

	flag.StringVar(&conf, "conf", "config.json", "read proxy config")
	flag.Parse()

	// 解析配置文件
	err = utils.ReadJsonConf(conf, &app_conf)
	if err != nil {
		klog.Errorf("read conf %s err %s", conf, err)
		return
	}

	klog.Infof("read proxy config %#v", app_conf)

	runListenr()
}
