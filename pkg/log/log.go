package log

import (
	"github.com/sirupsen/logrus"
	"os"
)

var Log = logrus.New()

func init() {
	//日志输出为标准输出
	Log.Out = os.Stdout
	//设置日志级别为debug级别
	Log.SetLevel(logrus.DebugLevel)
	// 输出格式
	Log.SetFormatter(&logrus.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
	// 是否追踪方法
	Log.SetReportCaller(false)
}
