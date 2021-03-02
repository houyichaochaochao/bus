package bus

import (
	"fmt"
	"testing"
	"time"
)


/**
 * json :

  {
  "createTime": "2019-12-24 17:43:04",
  "cron": "0 0 * * * ?",
  "group": "trade",
  "id": "d176488a-865f-4840-91c8-757ec6da20bf",
  "ip": "127.0.0.1",
  "jobId": "110",
  "mobile": "18758586911",
  "name": "第一个任务",
  "params": "我是参数",
  "remark": "备注",
  "target": "com.busgo.cat.job.EchoJob"
}


 */
func TestNewForestClient(t *testing.T) {

}

// 其实这里还可以封装一个函数 ,然后在client主函数中调用一下，就是自启了
func TestForestClient_Bootstrap(t *testing.T) {


	// 这里不需要改变，etcd固定，应该是从apollo拿一个vip，grindstone 应该是配置好了的
	etcd, _ := NewEtcd([]string{"127.0.0.1:2379"}, time.Second*10) // 这个可以都定死，或者依赖apollo，不过依赖apollo不好，这样造成了多级依赖，不好复用

	// 这里为啥不可以自己活得ip呢，像serve一样，应该也是可以的，然后集群就是从apollo获得这样看就没问题了，有个问题hna这种机房能否自己获得机房名字，或者直接写死？？？能否获得自己的机房名字可以和伟鸿确认下，要求自启之后就能把自己所在的机房和ip注册过去，看来不是那么容易！！！

	// 所以这里的写法不能满足目前grindstone的需求，我需要改写。  看来要固定机房了，不过固定机房还不好。 有没有检测机房的东西
	forestClient := NewForestClient("trade", "127.0.0.1", etcd)
	// 这个机房名字只能手写吗,我想不用集群，不用集群，只用ip的话应该也可以啊
	// 如果机器是内网不冲突的话，那么我可以不指定集群，直接指定ip，集群的名字都统一用ip


	// 这里不需要
	forestClient.PushJob("com.busgo.cat.job.EchoJob",&EchoJob{})
	// 这里不用变
	forestClient.Bootstrap()
}

type EchoJob struct {
	// 这里应该是一个jobConf啊，或者就是一个快照，这样我也能直接计算

}

func (*EchoJob) execute(params string) (string, error) {

	time.Sleep(time.Second * 5)
	fmt.Println("参数:", params) // 这里的接口应该是业务方自己实现的，原来如此啊！
	// 所以这里我可以自己添加curl 执行
	return "ok", nil
}
