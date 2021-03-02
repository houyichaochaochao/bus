package bus

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

const jobSnapshotPrefix = "/forest/client/snapshot/%s/%s/"
const jobExecuteSnapshotPrefix = "/forest/client/execute/snapshot/%s/%s/"

const (
	// 执行中
	JobExecuteDoingStatus = 1
	// 执行成功
	JobExecuteSuccessStatus = 2
	// 未知
	JobExecuteUkonwStatus = 3
	// 执行失败
	JobExecuteErrorStatus = -1
)

type JobSnapshotProcessor struct {
	etcd                *Etcd
	snapshotPath        string
	snapshotExecutePath string
	snapshots           chan *JobSnapshot
	jobs                map[string]Job // job 是接口，实现这个接口的都可以传进去
	lk                  *sync.RWMutex
}

// new a job snapshot processor
func NewJobSnapshotProcessor(group, ip string, etcd *Etcd) (*JobSnapshotProcessor) {

	processor := &JobSnapshotProcessor{
		etcd:      etcd,
		snapshots: make(chan *JobSnapshot, 100),
		jobs:      make(map[string]Job),
		lk:        &sync.RWMutex{},
	}
	processor.snapshotPath = fmt.Sprintf(jobSnapshotPrefix, group, ip)
	processor.snapshotExecutePath = fmt.Sprintf(jobExecuteSnapshotPrefix, group, ip)

	go processor.lookup()

	return processor
}

// lookup the job snapshot
func (processor *JobSnapshotProcessor) lookup() {

	for {

		select {
		case snapshot := <-processor.snapshots:

			go processor.handleSnapshot(snapshot)

		}
	}
}

func (processor *JobSnapshotProcessor) pushJobSnapshot(snapshot *JobSnapshot) {

	processor.snapshots <- snapshot // 推进去 给上面的函数用，才开始真正的处理，也就是通信的作用
}

// handle the snapshot
func (processor *JobSnapshotProcessor) handleSnapshot(snapshot *JobSnapshot) {

	target := snapshot.Target

	now := time.Now()

	executeSnapshot := &JobExecuteSnapshot{
		Id:         snapshot.Id,
		JobId:      snapshot.JobId,
		Name:       snapshot.Name,
		Group:      snapshot.Group,
		Ip:         snapshot.Ip,
		Cron:       snapshot.Cron,
		Target:     snapshot.Target,
		Params:     snapshot.Params,
		Status:     JobExecuteDoingStatus,
		CreateTime: snapshot.CreateTime,
		Remark:     snapshot.Remark,
		Mobile:     snapshot.Mobile,
		StartTime:  now.Format("2006-01-02 15:04:05"),
		Times:      0,
	}

	if target == "" { // 所以这个target只是用来识别的吗

		log.Printf("the snapshot:%v target is nil", snapshot)
		executeSnapshot.Status = JobExecuteUkonwStatus
		return
	}

	job, ok := processor.jobs[target] // 这个target也不是唯一的，这样不会出现重复性问题吗
	if !ok || job == nil {
		log.Printf("the snapshot:%v target is not found in the job list", snapshot)
		executeSnapshot.Status = JobExecuteUkonwStatus
	}

	value, _ := json.Marshal(executeSnapshot)

	key := processor.snapshotExecutePath + executeSnapshot.Id //这个是弄到作业上报目录，状态是正在执行中
	if err := processor.etcd.Put(key, string(value)); err != nil {
		log.Printf("the snapshot:%v put snapshot execute fail:%v", executeSnapshot, err) // 这里上传到任务作业上报目录  状态是执行中
		return
	}

	if executeSnapshot.Status !=JobExecuteDoingStatus { // 多线程的原因，别人会改这里吗，所以加这一个判断，因为上面接手处理的时候就是doing初始化的
		return
	}

	// 所以这里执行有问题，上面的参数如果足够的话， 我可以在这里直接开一个HTTP 的各种请求了，直接使用，然后方便多了，
	result, err := job.execute(snapshot.Params) // 只要参数就能执行吗，这里是业务方自己实现的，可是我们的grindstone是服务方直接实现的
	after := time.Now()
	executeSnapshot.Status = JobExecuteSuccessStatus
	executeSnapshot.Result = result
	if err != nil {
		executeSnapshot.Status = JobExecuteErrorStatus
	}

	duration := after.Sub(now)

	times := duration / time.Second
	executeSnapshot.Times = int(times)
	executeSnapshot.FinishTime = after.Format("2006-01-02 15:04:05")
	log.Printf("the execute snapshot:%v execute success ", executeSnapshot)

	// store the execute job snapshot
	value, _ = json.Marshal(executeSnapshot)
	if err := processor.etcd.Put(key, string(value)); err != nil {
		log.Printf("the snapshot:%v put update snapshot execute fail:%v", executeSnapshot, err) // 这里上传执行结果 // 但是前面已经将key删除了不是，这里即使失败了，那么也该有个client处理啊 // 这里上报就是
		return
	}

}

// push a job to job list
func (processor *JobSnapshotProcessor) PushJob(name string, job Job) {
	// 这个是后面处理快照的时候使用

	processor.lk.Lock()
	defer processor.lk.Unlock()

	if _, ok := processor.jobs[name]; ok {
		log.Printf("the job %s :%v  has exist!", name, job)
		return
	}

	processor.jobs[name] = job
}
