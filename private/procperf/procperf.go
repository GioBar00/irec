package procperf

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/scionproto/scion/pkg/log"
)

type Type string

const (
	Received      Type = "Received"
	ReceivedBcn   Type = "ReceivedBcn"
	Propagated    Type = "Propagated"
	PropagatedBcn Type = "PropagatedBcn"
	Originated    Type = "Originated"
	OriginatedBcn Type = "OriginatedBcn"
	Retrieved     Type = "Retrieved"
	Written       Type = "Written"
	Processed     Type = "Processed"
	Executed      Type = "Executed"
	Completed     Type = "Completed"
	Algorithm     Type = "Algorithm"

	maxTimeArraySize = 7
)

var file *os.File
var once sync.Once
var linesToWriteChan chan string
var running = false

type ProcPerf struct {
	t         Type
	id        string
	next_id   string
	data      string
	time      time.Time
	size      int
	durations []time.Duration
}

func (pp *ProcPerf) AddDurationT(t1, t2 time.Time) {
	if pp.size >= maxTimeArraySize {
		log.Error("ProcPerf size exceeded", "max", maxTimeArraySize)
		return
	}
	pp.durations = append(pp.durations, t2.Sub(t1))
	pp.size++
}

func (pp *ProcPerf) AddDuration(seconds float64) {
	if pp.size >= maxTimeArraySize {
		log.Error("ProcPerf size exceeded", "max", maxTimeArraySize)
		return
	}
	pp.durations = append(pp.durations, time.Duration(seconds*float64(time.Second)))
	pp.size++
}

func (pp *ProcPerf) SetNumBeacons(num uint32) {
	pp.data = fmt.Sprintf("%d", num)
}

func (pp *ProcPerf) SetData(data string) {
	pp.data = data
}

func (pp *ProcPerf) SetNextID(id string) {
	pp.next_id = id
}

func (pp *ProcPerf) SetID(id string) {
	pp.id = id
}

func (pp *ProcPerf) string() string {
	str := fmt.Sprintf("%s;%s;%s;%s;%s;%d;", pp.t, pp.id, pp.next_id, pp.data, pp.time.Format(time.RFC3339Nano), pp.size)
	for i := 0; i < maxTimeArraySize; i++ {
		if i < pp.size {
			str += fmt.Sprintf("%f;", pp.durations[i].Seconds())
		} else {
			str += ";"
		}
	}
	return str[:len(str)-1] + "\n"
}

func (pp *ProcPerf) Write() {
	go func() {
		defer log.HandlePanic()
		linesToWriteChan <- pp.string()
	}()
}

func Init() error {
	var err error = nil
	once.Do(func() {
		hostname, err := os.Hostname()
		if err != nil {
			log.Error("Error getting hostname", "err", err)
		}
		file, _ = os.OpenFile(fmt.Sprintf("procperf-%s.csv", hostname), os.O_CREATE|os.O_RDWR, 0666)
		header := "Type;ID;Next ID;Data;Time;Size;"
		for i := 0; i < maxTimeArraySize; i++ {
			header += fmt.Sprintf("Duration %d;", i)
		}
		header = header[:len(header)-1] + "\n"
		_, err = file.WriteString(header)
		if err != nil {
			log.Error("Error writing header", "err", err)
		}
		linesToWriteChan = make(chan string, 1000)
		running = true
		go func() {
			defer log.HandlePanic()
			run()
		}()
	})
	return err
}

func run() {
	for running {
		line := <-linesToWriteChan
		_, err := file.WriteString(line)
		if err != nil {
			log.Error("Error writing line", "err", err)
		}
	}
}

func Close() {
	running = false
	_ = file.Close()
}

func GetNew(t Type, id string) *ProcPerf {
	return &ProcPerf{t: t, id: id, time: time.Now(), size: 0}
}

func GetFullId(id string, segID uint16) string {
	return fmt.Sprintf("%s %04x", id, segID)
}
