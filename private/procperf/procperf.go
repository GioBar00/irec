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
)

// var beaconTime = sync.Map{}
var file *os.File
var once sync.Once

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
	})
	return err
}

func Close() {
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
	_, err := file.WriteString(ppt + ";" + id + ";" + newIdStr + ";" + start.Format(time.RFC3339Nano) + ";" + end.Format(time.RFC3339Nano) + "\n")
	return err
}

func GetFullId(id string, segID uint16) string {
	return fmt.Sprintf("%s %04x", id, segID)
}
