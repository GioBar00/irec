package ingress

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/control/irec/ingress/storage"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	"github.com/scionproto/scion/private/periodic"
)

var _ periodic.Task = (*JobHandler)(nil)
var _ RacJobHandler = (*JobHandler)(nil)

var retriesRacJobs = 0

const (
	defaultAgeingFactor    = 1.0
	defaultPullBasedFactor = 1.0
	defaultGroupSizeFactor = 1.0
)

type RacJob struct {
	RacJobAttr   *beacon.RacJobAttr
	Valid        bool
	LastExecuted time.Time
}

func (r *RacJob) Equal(r2 *RacJob) bool {
	return r.RacJobAttr.Equal(r2.RacJobAttr)
}

type PriorityQueueItem struct {
	RacJob *RacJob
	Index  int
}

type PriorityQueue []*PriorityQueueItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
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
	IngressDB storage.IngressStore

	racJobsByIsdAs map[addr.IA][]*RacJob

	normalRacJobs PriorityQueue
	pullRacJobs   PriorityQueue

	// Tick is mutable.
	Tick periodic.Tick
}

func (j *JobHandler) GetRacJob(ctx context.Context) (*beacon.RacJobAttr, error) {
	j.Lock()
	defer j.Unlock()
	if j.normalRacJobs.Len() == 0 && j.pullRacJobs.Len() == 0 {
		if retriesRacJobs > 0 {
			return nil, nil
		} else {
			if err := j.getNewRacJobs(ctx); err != nil {
				return nil, err
			}
			if j.normalRacJobs.Len() == 0 && j.pullRacJobs.Len() == 0 {
				retriesRacJobs++
				return nil, nil
			}
		}
	}
	var normal bool
	// choose random queue
	if j.pullRacJobs.Len() == 0 {
		normal = true
	} else if j.normalRacJobs.Len() == 0 {
		normal = false
	} else {
		normal = j.Tick.Now().UnixNano()%5 == 0
	}
	var racJob *RacJob
	if normal {
		racJob = heap.Pop(&j.normalRacJobs).(*PriorityQueueItem).RacJob
	} else {
		racJob = heap.Pop(&j.pullRacJobs).(*PriorityQueueItem).RacJob
	}
	racJob.LastExecuted = time.Now()
	racJob.Valid = false

	log.FromCtx(ctx).Info("Selected RacJob", "RacJob", racJob.RacJobAttr, "RemainingNormal", j.normalRacJobs.Len(), "RemainingPull", j.pullRacJobs.Len())

	return racJob.RacJobAttr, nil
}

func (j *JobHandler) Name() string {
	return "rac_job_updater"
}

func (j *JobHandler) Run(ctx context.Context) {
	j.Tick.SetNow(time.Now())
	if err := j.run(ctx); err != nil {
		log.FromCtx(ctx).Error("Error running job handler", "err", err)
	}
	//j.Tick.UpdateLast()
}

func (j *JobHandler) run(ctx context.Context) error {
	j.Lock()
	defer j.Unlock()
	retriesRacJobs = 0
	return j.getNewRacJobs(ctx)
}

func (j *JobHandler) getNewRacJobs(ctx context.Context) error {
	defer j.Tick.UpdateLast()
	if j.racJobsByIsdAs == nil {
		j.racJobsByIsdAs = make(map[addr.IA][]*RacJob)
	}
	// get new rac jobs
	newRacJobsAttr, err := j.IngressDB.GetValidRacJobs(ctx)
	if err != nil {
		return serrors.WrapStr("getting valid rac jobs", err)
	}
	// clear queues
	j.normalRacJobs = make(PriorityQueue, 0)
	j.pullRacJobs = make(PriorityQueue, 0)
	for _, racJobAttr := range newRacJobsAttr {
		// check if new isd-as
		_, ok := j.racJobsByIsdAs[racJobAttr.IsdAs]
		if !ok {
			j.racJobsByIsdAs[racJobAttr.IsdAs] = make([]*RacJob, 0)
		}
		// check if rac job already exists
		var racJob *RacJob
		for _, rj := range j.racJobsByIsdAs[racJobAttr.IsdAs] {
			if rj.RacJobAttr.Equal(racJobAttr) {
				racJob = rj
				break
			}
		}
		if racJob == nil {
			log.FromCtx(ctx).Info("New RacJob", "RacJob", racJobAttr)
			racJob = &RacJob{RacJobAttr: racJobAttr, Valid: true, LastExecuted: time.Now()}
			if !ok {
				racJob.LastExecuted = time.Now().Add(-5 * time.Minute)
			}
			j.racJobsByIsdAs[racJobAttr.IsdAs] = append(j.racJobsByIsdAs[racJobAttr.IsdAs], racJob)
		} else if racJobAttr.NotFetchCount >= 10 || racJob.LastExecuted.Add(1*time.Minute).Before(time.Now()) {
			racJob.Valid = true
			racJob.RacJobAttr.NotFetchCount = racJobAttr.NotFetchCount
		} else {
			racJob.RacJobAttr.NotFetchCount = racJobAttr.NotFetchCount
			continue
		}
		// add to queue
		if racJobAttr.PullBased {
			heap.Push(&j.pullRacJobs, &PriorityQueueItem{RacJob: racJob})
		} else {
			heap.Push(&j.normalRacJobs, &PriorityQueueItem{RacJob: racJob})
		}
	}
	return nil
}
