package procperf

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
)

type Type string

const (
	Received   Type = "Received"
	Propagated Type = "Propagated"
	Originated Type = "Originated"
	Retrieved  Type = "Retrieved"
	Written    Type = "Written"
	Processed  Type = "Processed"
	Executed   Type = "Executed"
	Completed  Type = "Completed"
	Algorithm  Type = "Algorithm"
)

var file *os.File
var once sync.Once
var linesToWriteChan chan string
var running bool = false

func Init() error {
	var err error = nil
	once.Do(func() {
		hostname, err := os.Hostname()
		if err != nil {
			log.Error("Error getting hostname", "err", err)
		}
		file, _ = os.OpenFile(fmt.Sprintf("procperf-%s.csv", hostname), os.O_CREATE|os.O_RDWR, 0666)
		_, err = file.WriteString("Type;ID;Next ID;Time Array\n")
		if err != nil {
			log.Error("Error writing header", "err", err)
		}
		linesToWriteChan = make(chan string, 1000)
		running = true
		go run()
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

func AddTimestampsDoneBeacon(id string, procPerfType Type, times []time.Time, newId ...string) error {
	if procPerfType == Propagated && len(newId) == 0 {
		return serrors.New("newId not found for propagated beacon")
	}
	newIdStr := ""
	if len(newId) > 0 {
		newIdStr = newId[0]
	}
	ppt := string(procPerfType)
	var timeStrings []string
	for _, t := range times {
		timeStrings = append(timeStrings, t.Format(time.RFC3339Nano))
	}
	timeStr := "[" + strings.Join(timeStrings, ",") + "]"
	_, err := file.WriteString(ppt + ";" + id + ";" + newIdStr + ";" + timeStr + "\n")
	return err
}

func AddTimeDoneBeacon(id string, procPerfType Type, start time.Time, end time.Time, newId ...string) error {
	if procPerfType == Propagated && len(newId) == 0 {
		return serrors.New("newId not found for propagated beacon")
	}
	newIdStr := ""
	if len(newId) > 0 {
		newIdStr = newId[0]
	}
	ppt := string(procPerfType)
	//_, err := file.WriteString(ppt + ";" + id + ";" + newIdStr + ";" + start.Format(time.RFC3339Nano) + ";" + end.Format(time.RFC3339Nano) + "\n")
	linesToWriteChan <- ppt + ";" + id + ";" + newIdStr + ";" + start.Format(time.RFC3339Nano) + ";" + end.Format(time.RFC3339Nano) + "\n"
	return nil
	//return err
}

func GetFullId(id string, segID uint16) string {
	return fmt.Sprintf("%s %04x", id, segID)
}
