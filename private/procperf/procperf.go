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
	Processed  Type = "Processed"
	Executed   Type = "Executed"
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

//func AddBeaconTime(id string, t time.Time) {
//	beaconTime.Store(id, t)
//}
//
//func DoneBeacon(id string, procPerfType Type, t time.Time, newId ...string) error {
//	if bt, ok := beaconTime.Load(id); ok {
//		bt := bt.(time.Time)
//		return AddTimeDoneBeacon(id, procPerfType, bt, t, newId...)
//	} else {
//		return serrors.New("beacon not found in beaconTime")
//	}
//}

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
	// log.Info(fmt.Sprintf("Beacon %s - ID:%s --- %s %s", ppt, id, t.String(), newIdStr))
	_, err := file.WriteString(ppt + ";" + id + ";" + newIdStr + ";" + start.Format(time.RFC3339Nano) + ";" + end.Format(time.RFC3339Nano) + "\n")
	//beaconTime.Delete(id)
	//return nil
	return err
}

func GetFullId(id string, segID uint16) string {
	return fmt.Sprintf("%s %04x", id, segID)
}
