package procperf

import (
	"fmt"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	"os"
	"sync"
	"time"
)

type Type string

const (
	Received   Type = "Received"
	Propagated Type = "Propagated"
	Originated Type = "Originated"
	Processed  Type = "Processed"
)

var beaconTime = make(map[string]time.Time)
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
		_, err = file.WriteString("ID; Next ID; Type; Start Time; End Time\n")
	})
	return err
}

func Close() {
	_ = file.Close()
}

func AddBeaconTime(id string, t time.Time) {
	beaconTime[id] = t
}

func DoneBeacon(id string, procPerfType Type, t time.Time, newId ...string) error {
	if _, ok := beaconTime[id]; ok {
		if procPerfType == Propagated && len(newId) == 0 {
			return serrors.New("newId not found for propagated beacon")
		}
		newIdStr := ""
		if len(newId) > 0 {
			newIdStr = newId[0]
		}
		ppt := string(procPerfType)
		_, err := file.WriteString(id + "; " + newIdStr + "; " + ppt + "; " + beaconTime[id].String() + "; " + t.String() + "\n")
		delete(beaconTime, id)
		return err
	} else {
		return serrors.New("beacon not found in beaconTime")
	}
}
