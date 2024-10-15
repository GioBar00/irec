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
	MakeRacJobValid(ctx context.Context, racJobAttr *beacon.RacJobAttr)
}

type RacJob struct {
	RacJobAttr              *beacon.RacJobAttr
	NotFetchCount           uint32
	Valid                   bool
	LastExecuted            time.Time
	MinPullBasedHyperPeriod time.Time
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

	normalRacJobs PriorityQueue
	pullRacJobs   PriorityQueue
}

func (j *JobHandler) addRacJobToQueue(ctx context.Context, racJob *RacJob) {
	queueItem := &PriorityQueueItem{RacJob: racJob}
	if racJob.RacJobAttr.PullBased {
		heap.Push(&j.pullRacJobs, queueItem)
	} else {
		heap.Push(&j.normalRacJobs, queueItem)
	}
	j.QueueItemByMapKey[MapKeyFrom(racJob.RacJobAttr)] = queueItem
}

func (j *JobHandler) MakeRacJobValid(ctx context.Context, racJobAttr *beacon.RacJobAttr) {
	j.Lock()
	defer j.Unlock()
	mapKey := MapKeyFrom(racJobAttr)
	log.FromCtx(ctx).Debug("MakeRacJobValid", "RacJob", racJobAttr)
	if _, ok := j.QueueItemByMapKey[mapKey]; !ok {
		if racJob, ok := j.RacJobByMapKey[mapKey]; ok {
			racJob.Valid = true
			//racJob.LastExecuted = racJob.LastExecuted.Add(-1 * time.Minute)
			j.addRacJobToQueue(ctx, racJob)
		} else {
			log.FromCtx(ctx).Info("Error: Trying to validate non-existent RacJob", "RacJob", racJob)
		}
		return
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
		racJob = &RacJob{RacJobAttr: beacon.RacJobAttr, LastExecuted: time.Now().Add(timeShift), MinPullBasedHyperPeriod: beacon.PullBasedHyperPeriod}
		j.RacJobByMapKey[mapKey] = racJob
	}
	racJob.NotFetchCount++
	// Check if Valid
	if racJob.RacJobAttr.PullBased {
		if beacon.PullBasedHyperPeriod.Before(racJob.MinPullBasedHyperPeriod) {
			racJob.MinPullBasedHyperPeriod = beacon.PullBasedHyperPeriod
		}
		if racJob.NotFetchCount < beacon.PullBasedMinBeacons || !racJob.MinPullBasedHyperPeriod.Before(time.Now()) {
			return
		}
	}
	racJob.Valid = true
	j.addRacJobToQueue(ctx, racJob)
}

func (j *JobHandler) GetRacJob(ctx context.Context) (*beacon.RacJobAttr, error) {
	j.Lock()
	defer j.Unlock()
	if j.normalRacJobs.Len() == 0 && j.pullRacJobs.Len() == 0 {
		return nil, nil
	}
	var normal bool
	// choose random queue
	if j.pullRacJobs.Len() == 0 {
		normal = true
	} else if j.normalRacJobs.Len() == 0 {
		normal = false
	} else {
		normal = time.Now().UnixNano()%5 == 0
	}
	var racJob *RacJob
	if normal {
		racJob = heap.Pop(&j.normalRacJobs).(*PriorityQueueItem).RacJob
	} else {
		racJob = heap.Pop(&j.pullRacJobs).(*PriorityQueueItem).RacJob
	}
	racJob.LastExecuted = time.Now()
	racJob.Valid = false
	racJob.NotFetchCount = 0
	delete(j.QueueItemByMapKey, MapKeyFrom(racJob.RacJobAttr))

	log.FromCtx(ctx).Info("Selected RacJob", "RacJob", racJob.RacJobAttr, "RemainingNormal", j.normalRacJobs.Len(), "RemainingPull", j.pullRacJobs.Len())

	return racJob.RacJobAttr, nil
}
