package racjob

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/log"
)

const (
	defaultAgeingFactor    = 1.0
	defaultPullBasedFactor = 1.0
	defaultGroupSizeFactor = 1.0
)

type RacJobHandler interface {
	GetRacJob(ctx context.Context) (*beacon.RacJobAttr, error)
	UpdateRacJob(ctx context.Context, beacon *beacon.BeaconAttr)
	PreMarkRacJob(ctx context.Context, racJobAttr *beacon.RacJobAttr)
	MarkRacJob(ctx context.Context, racJobAttr *beacon.RacJobAttr, failed int32, total int32)
}

type RacJob struct {
	RacJobAttr              *beacon.RacJobAttr
	NotFetchCount           uint32
	Valid                   bool
	FirstReceivedBeacon     time.Time
	LastExecuted            time.Time
	MinPullBasedHyperPeriod time.Time
	Executing               int
}

func (r *RacJob) Equal(r2 *RacJob) bool {
	return r.RacJobAttr.Equal(r2.RacJobAttr)
}

type MapKey struct {
	IsdAs           addr.IA
	IntfGroup       uint16
	AlgHash         string
	AlgId           uint32
	PullBased       bool
	PullTargetIsdAs addr.IA
}

func MapKeyFrom(racJobAttr *beacon.RacJobAttr) MapKey {
	return MapKey{
		IsdAs:           racJobAttr.IsdAs,
		IntfGroup:       racJobAttr.IntfGroup,
		AlgHash:         string(racJobAttr.AlgHash),
		AlgId:           racJobAttr.AlgId,
		PullBased:       racJobAttr.PullBased,
		PullTargetIsdAs: racJobAttr.PullTargetIsdAs,
	}
}

type PriorityQueueItem struct {
	RacJob *RacJob
	Index  int
}

type PriorityQueue []*PriorityQueueItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// TODO: Implement Priority based on RacJobAttr
	return pq[i].RacJob.LastExecuted.Before(pq[j].RacJob.LastExecuted)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PriorityQueueItem)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}

type JobHandler struct {
	sync.RWMutex

	SeenIsdAs         map[addr.IA]struct{}
	RacJobByMapKey    map[MapKey]*RacJob
	QueueItemByMapKey map[MapKey]*PriorityQueueItem
	ExecutingRacJobs  map[MapKey]*RacJob

	normalRacJobs PriorityQueue
	pullRacJobs   PriorityQueue
}

func (j *JobHandler) pushRacJobToQueue(ctx context.Context, racJob *RacJob) {
	racJob.Valid = true
	queueItem := &PriorityQueueItem{RacJob: racJob}
	if racJob.RacJobAttr.PullBased {
		heap.Push(&j.pullRacJobs, queueItem)
	} else {
		heap.Push(&j.normalRacJobs, queueItem)
	}
	j.QueueItemByMapKey[MapKeyFrom(racJob.RacJobAttr)] = queueItem
}

func (j *JobHandler) popRacJobFromQueue(ctx context.Context, pullBased bool) *RacJob {
	var racJob *RacJob
	if !pullBased {
		racJob = heap.Pop(&j.normalRacJobs).(*PriorityQueueItem).RacJob
	} else {
		racJob = heap.Pop(&j.pullRacJobs).(*PriorityQueueItem).RacJob
	}
	racJob.LastExecuted = time.Now()
	racJob.Valid = false
	racJob.NotFetchCount = 0
	racJob.Executing = 1
	mapKey := MapKeyFrom(racJob.RacJobAttr)
	delete(j.QueueItemByMapKey, mapKey)
	j.ExecutingRacJobs[mapKey] = racJob
	return racJob
}

func (j *JobHandler) MarkRacJob(ctx context.Context, racJobAttr *beacon.RacJobAttr, failed int32, total int32) {
	j.Lock()
	defer j.Unlock()
	mapKey := MapKeyFrom(racJobAttr)
	if racJob, ok := j.RacJobByMapKey[mapKey]; ok {
		log.FromCtx(ctx).Debug("RJ; Marking RacJob", "RacJobAttr", racJobAttr, "Failed", failed, "Total", total)
		if _, ok := j.ExecutingRacJobs[mapKey]; ok {
			delete(j.ExecutingRacJobs, mapKey)
			racJob.Executing = 0
		} else {
			log.FromCtx(ctx).Debug("RJ; RacJob not executing when Marking", "RacJobAttr", racJobAttr)
			return // Already added to queue
		}
		if failed > 0 || racJob.Valid {
			if failed > 0 {
				duration := time.Duration(60*failed/total) * time.Second
				racJob.LastExecuted = racJob.LastExecuted.Add(-duration)
			}
			j.pushRacJobToQueue(ctx, racJob)
		}
	} else {
		log.FromCtx(ctx).Error("RJ; Marking non-existent RacJob", "RacJobAttr", racJobAttr)
	}
}

func (j *JobHandler) PreMarkRacJob(ctx context.Context, racJobAttr *beacon.RacJobAttr) {
	j.Lock()
	defer j.Unlock()
	mapKey := MapKeyFrom(racJobAttr)
	if racJob, ok := j.RacJobByMapKey[mapKey]; ok {
		if _, ok := j.ExecutingRacJobs[mapKey]; ok {
			racJob.Executing = 2
		} else {
			log.FromCtx(ctx).Debug("RJ; RacJob not executing when PreMarking", "RacJobAttr", racJobAttr)
		}
	} else {
		log.FromCtx(ctx).Error("RJ; PreMarking non-existent RacJob", "RacJobAttr", racJobAttr)
	}
}

func (j *JobHandler) UpdateRacJob(ctx context.Context, beacon *beacon.BeaconAttr) {
	j.Lock()
	defer j.Unlock()
	mapKey := MapKeyFrom(beacon.RacJobAttr)
	// check if in Queue
	queueItem, ok := j.QueueItemByMapKey[mapKey]
	if ok {
		queueItem.RacJob.NotFetchCount++
		if queueItem.RacJob.RacJobAttr.PullBased {
			if beacon.PullBasedHyperPeriod.Before(queueItem.RacJob.MinPullBasedHyperPeriod) {
				queueItem.RacJob.MinPullBasedHyperPeriod = beacon.PullBasedHyperPeriod
			}
			heap.Fix(&j.pullRacJobs, queueItem.Index)
		} else {
			heap.Fix(&j.normalRacJobs, queueItem.Index)
		}
		return
	}
	var timeShift time.Duration
	// check if new isd-as
	_, ok = j.SeenIsdAs[beacon.RacJobAttr.IsdAs]
	if !ok {
		j.SeenIsdAs[beacon.RacJobAttr.IsdAs] = struct{}{}
		timeShift = -4 * time.Minute
	}
	// check if rac job already exists
	racJob, ok := j.RacJobByMapKey[mapKey]
	if !ok {
		timeShift = timeShift - 1*time.Minute
		racJob = &RacJob{RacJobAttr: beacon.RacJobAttr, LastExecuted: time.Now().Add(timeShift), MinPullBasedHyperPeriod: beacon.PullBasedHyperPeriod, FirstReceivedBeacon: time.Now()}
		j.RacJobByMapKey[mapKey] = racJob
	} else if !racJob.Valid {
		racJob.FirstReceivedBeacon = time.Now()
	}
	racJob.NotFetchCount++
	// Check if Valid
	if racJob.RacJobAttr.PullBased {
		if beacon.PullBasedHyperPeriod.Before(racJob.MinPullBasedHyperPeriod) {
			racJob.MinPullBasedHyperPeriod = beacon.PullBasedHyperPeriod
		}
		if racJob.NotFetchCount >= beacon.PullBasedMinBeacons && racJob.MinPullBasedHyperPeriod.Before(time.Now()) {
			j.pushRacJobToQueue(ctx, racJob)
		}
	}
}

func (j *JobHandler) checkExecutingRacJobs(ctx context.Context) {
	for mapKey, racJob := range j.ExecutingRacJobs {
		if racJob.Executing == 1 && time.Since(racJob.LastExecuted) >= 30*time.Second {
			log.FromCtx(ctx).Info("RJ; RacJob Execution Timeout", "RacJobAttr", racJob.RacJobAttr)
			delete(j.ExecutingRacJobs, mapKey)
			racJob.Executing = 0
			racJob.LastExecuted = time.Now().Add(-1 * time.Minute)
			j.pushRacJobToQueue(ctx, racJob)
		}
	}
}

func (j *JobHandler) checkForValidJobs(ctx context.Context) {
	for _, racJob := range j.RacJobByMapKey {
		if !racJob.Valid && racJob.Executing == 0 && time.Since(racJob.FirstReceivedBeacon) >= 5*time.Second {
			j.pushRacJobToQueue(ctx, racJob)
		}
	}
}

func (j *JobHandler) GetRacJob(ctx context.Context) (*beacon.RacJobAttr, error) {
	j.Lock()
	defer j.Unlock()
	j.checkExecutingRacJobs(ctx)
	if j.normalRacJobs.Len() == 0 && j.pullRacJobs.Len() == 0 {
		return nil, nil
	}
	var pullBased bool
	// choose random queue
	if j.normalRacJobs.Len() > 0 && j.pullRacJobs.Len() > 0 {
		pullBased = time.Now().UnixNano()%5 == 0
	} else {
		pullBased = j.pullRacJobs.Len() > 0
	}
	racJob := j.popRacJobFromQueue(ctx, pullBased)
	log.FromCtx(ctx).Info("RJ; Selected RacJob", "RacJobAttr", racJob.RacJobAttr, "RemainingNormal", j.normalRacJobs.Len(), "RemainingPull", j.pullRacJobs.Len())

	return racJob.RacJobAttr, nil
}
