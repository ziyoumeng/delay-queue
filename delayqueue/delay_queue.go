package delayqueue

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/ouqiang/delay-queue/config"
)

var (
	// 每个定时器对应一个bucket
	timers []*time.Ticker
	// bucket名称chan
	bucketNameChan <-chan string
)

// Init 初始化延时队列
func Init() {
	RedisPool = initRedisPool()
	initTimers()
	bucketNameChan = generateBucketName()
}

// Push 添加一个Job到队列中
func Push(job Job) error {
	if job.Id == "" || job.Topic == "" || job.Delay < 0 || job.TTR <= 0 {
		return errors.New("invalid job")
	}

	err := putJob(job.Id, job)
	if err != nil {
		log.Printf("添加job到job pool失败#job-%+v#%s", job, err.Error())
		return err
	}
	// todo 失败了要删除job
	err = pushToBucket(<-bucketNameChan, job.Delay, job.Id)
	if err != nil {
		log.Printf("添加job到bucket失败#job-%+v#%s", job, err.Error())
		return err
	}

	return nil
}

// Pop 轮询获取Job
// todo  每次只能取的一个job。如果ready job较多的时候会加大网络I/O的消耗;改为批量获取
func Pop(topics []string) (*Job, error) {
	jobId, err := blockPopFromReadyQueue(topics, config.Setting.QueueBlockTimeout)
	if err != nil {
		return nil, err
	}

	// 队列为空
	if jobId == "" {
		return nil, nil
	}

	// 获取job元信息
	job, err := getJob(jobId)
	if err != nil {
		return job, err
	}

	// 消息不存在, 可能已被删除
	if job == nil {
		return nil, nil
	}

	timestamp := time.Now().Unix() + job.TTR
	err = pushToBucket(<-bucketNameChan, timestamp, job.Id)

	return job, err
}

// Remove 删除Job
func Remove(jobId string) error {
	return removeJob(jobId)
}

// Get 查询Job
func Get(jobId string) (*Job, error) {
	job, err := getJob(jobId)
	if err != nil {
		return job, err
	}

	// 消息不存在, 可能已被删除
	if job == nil {
		return nil, nil
	}
	return job, err
}

// 轮询获取bucket名称, 使job分布到不同bucket中, 提高扫描速度
func generateBucketName() <-chan string {
	c := make(chan string)
	go func() {
		i := 1
		for {
			c <- fmt.Sprintf(config.Setting.BucketName, i)
			if i >= config.Setting.BucketSize {
				i = 1
			} else {
				i++
			}
		}
	}()

	return c
}

// 初始化定时器

//todo timer的每1秒扫描一次延时队列，准度不高；改为afterFunc方式，精确定时扫描
func initTimers() {
	timers = make([]*time.Ticker, config.Setting.BucketSize)
	var bucketName string
	for i := 0; i < config.Setting.BucketSize; i++ {
		timers[i] = time.NewTicker(1 * time.Second)
		bucketName = fmt.Sprintf(config.Setting.BucketName, i+1)
		go waitTicker(timers[i], bucketName)
	}
}

func waitTicker(timer *time.Ticker, bucketName string) {
	for {
		select {
		case t := <-timer.C:
			tickHandler(t, bucketName)
		}
	}
}

// 扫描bucket, 取出延迟时间小于当前时间的Job
// todo 目前采用的是集中存储机制，在多实例部署时Timer程序可能会并发执行，导致job被重复放入ready queue。
//  为了解决这个问题，使用redis的setnx命令实现了简单的分布式锁，以保证每个bucket每次只有一个timer thread来扫描。
func tickHandler(t time.Time, bucketName string) {
	for {
		bucketItem, err := getFromBucket(bucketName)
		if err != nil {
			log.Printf("扫描bucket错误#bucket-%s#%s", bucketName, err.Error())
			return
		}

		// 集合为空
		if bucketItem == nil {
			return
		}

		// 延迟时间未到
		if bucketItem.timestamp > t.Unix() {
			return
		}

		// 延迟时间小于等于当前时间, 取出Job元信息并放入ready queue
		job, err := getJob(bucketItem.jobId)
		if err != nil {
			log.Printf("获取Job元信息失败#bucket-%s#%s", bucketName, err.Error())
			continue
		}

		// job元信息不存在, 从bucket中删除
		if job == nil {
			removeFromBucket(bucketName, bucketItem.jobId)
			continue
		}

		// 再次确认元信息中delay是否小于等于当前时间
		if job.Delay > t.Unix() {
			// 从bucket中删除旧的jobId
			removeFromBucket(bucketName, bucketItem.jobId)
			// 重新计算delay时间并放入bucket中
			pushToBucket(<-bucketNameChan, job.Delay, bucketItem.jobId)
			continue
		}
		//todo   如果pushToReadyQueue执行后,removeFromBucket还未执行时,业务调用了pop(pop会计算ttr重新将job放入bucket),bucket可能会丢失该job
		//todo   两种方案，1、lua脚本将pushToReadyQueue和removeFromBucket原子化；2、独立一个bucket专存rrt的job
		//   https://github.com/ouqiang/delay-queue/issues/19
		err = pushToReadyQueue(job.Topic, bucketItem.jobId)
		if err != nil {
			log.Printf("JobId放入ready queue失败#bucket-%s#job-%+v#%s",
				bucketName, job, err.Error())
			continue
		}

		// 从bucket中删除
		removeFromBucket(bucketName, bucketItem.jobId)
	}
}
