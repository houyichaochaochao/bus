package bus

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

const clientPathPrefix = "/forest/client/%s/clients/%s"
const (
	URegistryState = iota
	RegistryState
)

type ForestClient struct {
	etcd         *Etcd
	jobs         map[string]Job
	group        string
	ip           string
	running      bool
	quit         chan bool
	state        int
	clientPath   string
	snapshotPath string
	txResponse   *TxResponse

	snapshotProcessor *JobSnapshotProcessor
}

// new a client
func NewForestClient(group, ip string, etcd *Etcd) (*ForestClient) {

	return &ForestClient{
		etcd:  etcd,
		group: group,
		ip:    ip, // 所以这个ip应该是自己获取本机的最好？？？？
		jobs:  make(map[string]Job, 0),
		quit:  make(chan bool, 0),
		state: URegistryState,
	}

}

// bootstrap client
func (client *ForestClient) Bootstrap() (err error) {

	if err = client.validate(); err != nil {
		return err
	}

	client.clientPath = fmt.Sprintf(clientPathPrefix, client.group, client.ip)
	client.snapshotPath = fmt.Sprintf(jobSnapshotPrefix, client.group, client.ip)

	client.snapshotProcessor = NewJobSnapshotProcessor(client.group, client.ip, client.etcd) // 这里开始等待通道给snapshot

	client.addJobs() // 这个addJob 没用吧

	go client.registerNode() // 注册后 serve 端会随机选择一个client进行任务推送
	go client.lookup() // 无限循环的任务 // 这里也会自己拿 // 这里开始从etcd拿信息，给上面的处理通道

	client.running = true
	<-client.quit // 除非主动发送，不然一直阻塞在这里
	client.running = false
	return
}

// stop  client
func (client *ForestClient) Stop() {
	if client.running {
		client.quit <- true
	}

	return
}

// add jobs
func (client *ForestClient) addJobs() {

	if len(client.jobs) == 0 {
		return
	}

	for name, job := range client.jobs { // 这里添加到processor

		client.snapshotProcessor.PushJob(name, job)
	}
}

// pre validate params
func (client *ForestClient) validate() (err error) {

	if client.ip == "" {
		return errors.New(fmt.Sprint("ip not allow null"))
	}

	if client.group == "" {
		return errors.New(fmt.Sprint("group not allow null"))
	}
	return
}

// push a new job to job list
func (client ForestClient) PushJob(name string, job Job) (err error) {
	// 所以这个PushJob 只是当前客户端自己用的,把job写好就行了，格式什么的也都是自由的
	// 分布式调度中心也不会看到这个东西，因为这个不是调度中心分发的

	if client.running {
		return errors.New(fmt.Sprintf("the forest client is running not allow push a job "))
	}
	if _, ok := client.jobs[name]; ok {
		return errors.New(fmt.Sprintf("the job %s name exist!", name))
	}
	client.jobs[name] = job

	return
}

func (client *ForestClient) registerNode() {

RETRY:
	// 先定义绝对是一个好习惯
	var (
		txResponse *TxResponse
		err        error
	)

	if client.state == RegistryState {
		log.Printf("the forest client has already registry to:%s", client.clientPath)
		return
	}
	if txResponse, err = client.etcd.TxKeepaliveWithTTL(client.clientPath, client.ip, 10); err != nil { // 这里面互相嵌套
		log.Printf("the forest client fail registry to:%s", client.clientPath)
		time.Sleep(time.Second * 3) // 这里是三秒尝试通讯，原来还是要手工通讯
		goto RETRY // 注册失败  就重试
	}

	if !txResponse.Success {
		log.Printf("the forest client fail registry to:%s", client.clientPath)
		time.Sleep(time.Second * 3)
		goto RETRY // 重试
	}

	log.Printf("the forest client success registry to:%s", client.clientPath)
	client.state = RegistryState
	client.txResponse = txResponse // 所以后面的代码就是防止断开连接的
	// 又有一个问题，我注册的节点失联了，会自己消失吗

	// 因为这里会阻塞，所以正常，节点的状态只有两种，注册工程，或者失败
	select {
	case <-client.txResponse.StateChan: // 这里会阻塞 因为只有失败的时候会传回来
		client.state = URegistryState
		log.Printf("the forest client fail registry to----->:%s", client.clientPath)
		goto RETRY // 一旦失联了，就要重新注册
	}

}

// look up
func (client *ForestClient) lookup() {

	for {

		// 这里得到一个任务
		keys, values, err := client.etcd.GetWithPrefixKeyLimit(client.snapshotPath, 50) // 这里是限制50个？？  这里就是从任务快照拿，执行了才是到任务作业上报目录，所以现在还是快照目录

		if err != nil {

			log.Printf("the forest client load job snapshot error:%v", err)
			time.Sleep(time.Second * 3)
			continue
		}

		if client.state == URegistryState {
			time.Sleep(time.Second * 3)
			continue
		}

		if len(keys) == 0 || len(values) == 0 {
			log.Printf("the forest client :%s load job snapshot is empty", client.clientPath)
			time.Sleep(time.Second * 3)
			continue
		}

		for i := 0; i < len(values); i++ {

			key := keys[i] // key 对应的是id

			if client.state == URegistryState { // 这里还要检测，因为有可能挂掉了
				time.Sleep(time.Second * 3)
				break
			}

			// 这里就删除是不是早了 ,拿到了就产出这个任务，这个任务只有这个节点有
			if err := client.etcd.Delete(string(key)); err != nil {
				log.Printf("the forest client :%s delete job snapshot fail:%v", client.clientPath, err) // 把取到的key[i]从etcd删除掉？？？  // 这里删除的是任务快照的，所以删除了以后怎么办呢，没看到解决办法，没有尝试
				continue // 因为前面是用前缀取的，所以能控制数量 ,昨晚就删除了，可是要是没有完成怎么办
			}

			value := values[i]
			if len(value) == 0 { // 空的就算处理完了,因为下面执行失败是误解的，所以这里就可以直接删除了？？？？我的理解是这样的
				log.Printf("the forest client :%s found job snapshot value is nil ", client.clientPath)
				continue
			}

			snapshot := new(JobSnapshot)
			err := json.Unmarshal(value, snapshot)
			if err != nil {

				log.Printf("the forest client :%s found job snapshot value is cant not parse the json value：%v ", client.clientPath, err)
				continue
			}

			// push a job snapshot
			client.snapshotProcessor.pushJobSnapshot(snapshot) // 正式的推送服务

		}

	}
}
