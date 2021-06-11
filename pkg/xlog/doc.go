// 1 使用简介：
// 通过创建Output，配置到logger里头，支持同时配置多个Output
// (1) 输出到console
// xylog.AddOutput(xylog.NewConsoleWriter())
// xylog.Debug("hello %s %d", "world cpu", 2018)
//
// (2) 输出到文件，通过json指定配置信息
// w, _ := xylog.NewFileWriter(`{
//			"filepath":"../log/"
// 			"filename":"wei.log",
// 			"rotatesize":102400000
// 		}`)
// xylog.AddOutput(w)
//
// (3) 文件的主要配置项
// filepath: 日志文件夹路径
// filename: 日志名称，filepath+filename最终组成文件的真实位置
//
// 2 异步使用
// 通过调用Async()指定为异步模式，Async接受缓冲长度参数，如果不指定则默认是102400
// 异步方式通过写入channel由异步routine去消费写日志，采用异步的方式在程序退出的时候
// 需要显示调用Close函数,将channel的内容进行flush并销毁资源
//
// 3 Hook
// 如果想对消息内容做定制输出，或者进行一些额外的处理，可以实现LogHook接口，如下：
// func (h *MyHook) Hook(lv xylog2.Level, msg *string) error {
//	*msg = "[fuck] " + *msg
// 	if lv == ErrorLevel {
// 	do somethin
// 	}
//	return nil
// }
// xylog.AddHook(&MyHook{})
//
// 4 兼容老的使用方式
// 新的log兼容老的使用方式，如下，但目前只支持全局log的配置，即下面的函数只针对全局log
// 	xylog.SetLogFilePath("./", "test.log")
//	xylog.SetLogRotateFileSize(1024 * 1024 * 100)
//
// 5 关于性能
// 相比老的log，新的log减少了两个系统调用，性能损耗相对少一些，实测效果比老的好些。
// 如果程序对性能要求较高可以采用异步的方式，在async的buffer未满的时候，性能是同步的3~4倍。
// 以下是对echo功能的http服务做的压测性能：
// no log :7.6w
// new log async: 5.8w
// new log sync: 3.2w
// old log: 2.5

package xlog
