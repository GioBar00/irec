//go:build !wa && !waopt && !native

// Copyright 2019 Anapaya Systems
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	"github.com/scionproto/scion/private/storage/db"
	beacon2 "github.com/scionproto/scion/private/storage/ingress"
)

// DYNAMIC RACS
func (e *executor) GetBeaconJob(ctx context.Context, ignoreIntfGroup bool, fetchExpirationTime time.Time) ([][]byte, []*cppb.IRECBeaconUnopt, []byte, []int64, error) {
	query := `SELECT b2.RowID, b2.LastUpdated, b2.InIntfID, b2.Usage, b2.Flatbuffer, b2.AlgorithmHash, b2.Beacon  from Beacons b2,
		(SELECT DISTINCT b.StartIsd, b.StartAs, b.StartIntfGroup, b.AlgorithmHash, b.AlgorithmId, b.PullBased, b.PullBasedTargetAs, b.PullBasedTargetIsd
FROM Beacons b, Algorithm a
WHERE FetchStatus = 0 AND b.AlgorithmHash = a.AlgorithmHash
GROUP BY b.StartIsd, b.StartAs, b.StartIntfGroup, b.AlgorithmHash, b.AlgorithmId, b.PullBased, b.PullBasedTargetAs, b.PullBasedTargetIsd
HAVING COUNT(b.RowID) > b.PullBasedMinBeacons AND min(b.PullBasedPeriod) <= ?
ORDER BY RANDOM()
LIMIT 1)
		    selected WHERE b2.StartIsd = selected.StartIsd AND b2.StartAs = selected.StartAs  AND b2.AlgorithmHash = selected.AlgorithmHash AND b2.AlgorithmId = selected.AlgorithmId AND b2.PullBased = selected.PullBased AND b2.PullBasedTargetIsd = selected.PullBasedTargetIsd AND b2.PullBasedTargetAs = selected.PullBasedTargetAs`
	if !ignoreIntfGroup {
		query = query + ` AND b2.StartIntfGroup = selected.StartIntfGroup`
	}
	query = query + ` ORDER BY b2.HopsLength ASC, b2.LastUpdated DESC;`

	//_, err := e.db.ExecContext(ctx, query, time.Now().UnixNano())
	//if err != nil {
	//	return nil, []byte{}, []int64{}, err
	//}

	rows, err := e.db.QueryContext(ctx, query, time.Now().UnixNano())
	if err != nil {
		return [][]byte{}, []*cppb.IRECBeaconUnopt{}, []byte{}, []int64{}, serrors.WrapStr("looking up beacons", err, "query", query)
	}
	defer rows.Close()
	var algHash sql.RawBytes
	var fbs [][]byte
	var rowIds []int64
	var res []*cppb.IRECBeaconUnopt
	count := int64(0)
	for rows.Next() {
		var lastUpdated int64
		var InIntfID uint16
		var rowId int64
		var usage beacon.Usage
		var rawBeacon sql.RawBytes
		var flatbuffer sql.RawBytes

		err = rows.Scan(&rowId, &lastUpdated, &InIntfID, &usage, &flatbuffer, &algHash, &rawBeacon)
		if err != nil {
			return [][]byte{}, []*cppb.IRECBeaconUnopt{}, []byte{}, []int64{}, serrors.WrapStr("reading row", err)
		}
		rowIds = append(rowIds, rowId)
		fbs = append(fbs, flatbuffer)
		seg, err := beacon.UnpackBeaconPB(rawBeacon)

		if err != nil {
			return nil, []*cppb.IRECBeaconUnopt{}, []byte{}, []int64{}, serrors.WrapStr("parsing beacon", err)
		}
		res = append(res, &cppb.IRECBeaconUnopt{
			PathSeg: seg,
			InIfId:  uint32(InIntfID),
			Id:      count,
		})
		count += 1
	}

	if err := rows.Err(); err != nil {
		log.Error("err when selecting beacons", "err", err)
		return [][]byte{}, []*cppb.IRECBeaconUnopt{}, []byte{}, []int64{}, err
	}

	_, err = e.db.ExecContext(ctx, fmt.Sprintf("UPDATE Beacons SET FetchStatus = 0, FetchStatusExpirationTime=?  WHERE RowID in (%s);", strings.Replace(strings.Trim(fmt.Sprint(rowIds), "[]"), " ", ",", -1)), fetchExpirationTime.Unix())
	if err != nil {
		return [][]byte{}, []*cppb.IRECBeaconUnopt{}, []byte{}, []int64{}, err
	}
	return fbs, res, algHash, rowIds, nil

	//e.Lock()
	//defer e.Unlock()
	//tx, err := e.db.(*sql.DB).BeginTx(ctx, nil)
	//if err != nil {
	//	return [][]byte{}, []byte{}, []int64{}, serrors.WrapStr("looking up beacons", err)
	//}
	//_, err = tx.ExecContext(ctx, "CREATE TEMP TABLE Retrieved(RowId INTEGER);")
	//if err != nil {
	//	return nil, []byte{}, []int64{}, err
	//}
	//(SELECT DISTINCT b.StartIsd, b.StartAs, b.StartIntfGroup, b.AlgorithmHash, b.AlgorithmId FROM Beacons b, Algorithm a WHERE FetchStatus = 0 AND b.AlgorithmHash = a.AlgorithmHash ORDER BY RANDOM() LIMIT 1)
	//	query := `INSERT INTO Retrieved(RowID)
	//	SELECT DISTINCT b2.RowID from Beacons b2,
	//		(SELECT DISTINCT b.StartIsd, b.StartAs, b.StartIntfGroup, b.AlgorithmHash, b.AlgorithmId, b.PullBased, b.PullBasedTargetAs, b.PullBasedTargetIsd
	//FROM Beacons b, Algorithm a
	//WHERE FetchStatus = 0 AND b.AlgorithmHash = a.AlgorithmHash
	//GROUP BY b.StartIsd, b.StartAs, b.StartIntfGroup, b.AlgorithmHash, b.AlgorithmId, b.PullBased, b.PullBasedTargetAs, b.PullBasedTargetIsd
	//HAVING COUNT(b.RowID) > b.PullBasedMinBeacons AND min(b.PullBasedPeriod) <= ?
	//ORDER BY RANDOM()
	//LIMIT 1)
	//		    selected WHERE b2.StartIsd = selected.StartIsd AND b2.StartAs = selected.StartAs  AND b2.AlgorithmHash = selected.AlgorithmHash AND b2.AlgorithmId = selected.AlgorithmId AND b2.PullBased = selected.PullBased AND b2.PullBasedTargetIsd = selected.PullBasedTargetIsd AND b2.PullBasedTargetAs = selected.PullBasedTargetAs`
	//	if !ignoreIntfGroup {
	//		query = query + ` AND b2.StartIntfGroup = selected.StartIntfGroup`
	//	}
	//
	//	_, err = tx.ExecContext(ctx, query, time.Now().UnixNano())
	//	if err != nil {
	//		return nil, []byte{}, []int64{}, err
	//	}
	//	_, err = tx.ExecContext(ctx, "UPDATE Beacons SET FetchStatus = 0, FetchStatusExpirationTime=? FROM Retrieved r WHERE Beacons.RowID = r.RowId AND Beacons.FetchStatus =0;", fetchExpirationTime.Unix())
	//
	//	if err != nil {
	//		return nil, []byte{}, []int64{}, err
	//	}
	//
	//	rows, err := tx.QueryContext(ctx, "SELECT DISTINCT b.RowID, LastUpdated, InIntfID, Usage, Flatbuffer, AlgorithmHash FROM Beacons b, Retrieved r  WHERE b.RowID = r.RowId ORDER BY b.HopsLength ASC, b.LastUpdated DESC;")
	//	if err != nil {
	//		return [][]byte{}, []byte{}, []int64{}, serrors.WrapStr("looking up beacons", err, "query", query)
	//	}
	//	defer rows.Close()
	//	var algHash sql.RawBytes
	//	var fbs [][]byte
	//	var rowIds []int64
	//	for rows.Next() {
	//		var lastUpdated int64
	//		var InIntfID uint16
	//		var rowId int64
	//		var usage beacon.Usage
	//		var flatbuffer sql.RawBytes
	//
	//		err = rows.Scan(&rowId, &lastUpdated, &InIntfID, &usage, &flatbuffer, &algHash)
	//		if err != nil {
	//			return [][]byte{}, []byte{}, []int64{}, serrors.WrapStr("reading row", err)
	//		}
	//		rowIds = append(rowIds, rowId)
	//		fbs = append(fbs, flatbuffer)
	//	}
	//
	//	if err := rows.Err(); err != nil {
	//		log.Error("err when selecting beacons", "err", err)
	//		return [][]byte{}, []byte{}, []int64{}, err
	//	}
	//	_, err = tx.ExecContext(ctx, "DROP TABLE Retrieved;")
	//
	//	if err != nil {
	//		return nil, []byte{}, []int64{}, err
	//	}
	//
	//	err = tx.Commit()
	//	if err != nil {
	//		return nil, []byte{}, []int64{}, err
	//	}
	//
	//	return fbs, algHash, rowIds, nil
}

// DEPRECATED
func (e *executor) GetAndMarkBeacons(ctx context.Context, maximum uint32, algHash []byte, algId uint32, originAS addr.IA, originIntfGroup uint32, ignoreIntfGroup bool, fetchStatus uint32) ([][]byte, []*cppb.IRECBeaconUnopt, error) {
	//e.Lock() // TODO(jvb): DB transaction instead of lock
	//defer e.Unlock()
	selStmt, selArgs, updStmt, updArgs := e.buildSelUpdQuery(ctx, maximum, algHash, algId, originAS, originIntfGroup, ignoreIntfGroup, fetchStatus)
	rows, err := e.db.QueryContext(ctx, selStmt, selArgs...)
	if err != nil {
		return [][]byte{}, []*cppb.IRECBeaconUnopt{}, serrors.WrapStr("looking up beacons", err, "query", selStmt)
	}
	defer rows.Close()
	var fbs [][]byte
	var count int64 = 0
	var res []*cppb.IRECBeaconUnopt
	for rows.Next() {
		var RowID int
		var lastUpdated int64
		var InIntfID uint16
		var usage beacon.Usage
		var flatbuffer sql.RawBytes
		var rawBeacon sql.RawBytes
		err = rows.Scan(&RowID, &lastUpdated, &InIntfID, &usage, &flatbuffer, &rawBeacon)
		if err != nil {
			return [][]byte{}, []*cppb.IRECBeaconUnopt{}, serrors.WrapStr("reading row", err)
		}
		fbs = append(fbs, flatbuffer)
		seg, err := beacon.UnpackBeaconPB(rawBeacon)

		if err != nil {
			return [][]byte{}, []*cppb.IRECBeaconUnopt{}, serrors.WrapStr("parsing beacon", err)
		}
		res = append(res, &cppb.IRECBeaconUnopt{
			PathSeg: seg,
			InIfId:  uint32(InIntfID),
			Id:      count,
		})
		count += 1
	}

	if err := rows.Err(); err != nil {
		log.Error("err when selecting beacons", "err", err)
		return [][]byte{}, []*cppb.IRECBeaconUnopt{}, err
	}
	_, err = e.db.ExecContext(ctx, updStmt, updArgs...)
	if err != nil {
		log.Error("err when updating fetchStatus", "err", err)
		return [][]byte{}, []*cppb.IRECBeaconUnopt{}, db.NewWriteError("updating fetchStatuses", err)
	}
	return fbs, res, nil

}
func (e *executor) buildSelUpdQuery(ctx context.Context, maximum uint32, algHash []byte, algId uint32, originAS addr.IA, originIntfGroup uint32, ignoreIntfGroup bool, fetchStatus uint32) (string, []interface{}, string, []interface{}) {
	var selArgs []interface{}
	var updArgs []interface{}
	selQuery := "SELECT DISTINCT RowID, LastUpdated, InIntfID, Usage, Flatbuffer FROM Beacons"
	updQuery := "UPDATE Beacons SET FetchStatus = ?"
	updArgs = append(updArgs, fetchStatus)
	where := []string{}

	where = append(where, "(StartIsd=? AND StartAs=?)")
	selArgs = append(selArgs, originAS.ISD(), originAS.AS())
	updArgs = append(updArgs, originAS.ISD(), originAS.AS())
	//StartIntfGroup, AlgorithmHash
	where = append(where, "(AlgorithmHash=? AND AlgorithmID=?)")
	selArgs = append(selArgs, algHash, algId)
	updArgs = append(updArgs, algHash, algId)
	if !ignoreIntfGroup {
		where = append(where, "(StartIntfGroup=?)")
		selArgs = append(selArgs, originIntfGroup)
		updArgs = append(updArgs, originIntfGroup)
	}

	if len(where) > 0 {
		selQuery += "\n" + fmt.Sprintf("WHERE %s", strings.Join(where, " AND\n"))
		selQuery += "AND FetchStatus = 0"
		updQuery += "\n" + fmt.Sprintf("WHERE %s", strings.Join(where, " AND\n"))
	} else {
		selQuery += "\n WHERE FetchStatus = 0"
	}

	if maximum > 0 {
		selQuery += "\n LIMIT ?"
		updQuery += "\n LIMIT ?"
		selArgs = append(selArgs, maximum)
		updArgs = append(updArgs, maximum)
	}
	selQuery += "\n" + "ORDER BY LastUpdated DESC"
	return selQuery, selArgs, updQuery, updArgs
}

// USED BY STATIC RACS
func (e *executor) GetBeacons(ctx context.Context, opts *beacon2.QueryOptions) ([][]byte, []*cppb.IRECBeaconUnopt, error) {
	e.RLock()
	defer e.RUnlock()
	stmt, args := e.buildQuery(ctx, opts)
	log.Info("Formed query was", "stmt", stmt)
	rows, err := e.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return [][]byte{}, []*cppb.IRECBeaconUnopt{}, serrors.WrapStr("looking up beacons", err, "query", stmt)
	}
	defer rows.Close()
	var fbs [][]byte
	var res []*cppb.IRECBeaconUnopt
	count := int64(0)
	for rows.Next() {
		var RowID int64
		var lastUpdated int64
		var InIntfID uint16
		var usage beacon.Usage
		var flatbuffer sql.RawBytes
		var rawBeacon sql.RawBytes
		err = rows.Scan(&RowID, &lastUpdated, &InIntfID, &usage, &flatbuffer, &rawBeacon)
		if err != nil {
			return [][]byte{}, []*cppb.IRECBeaconUnopt{}, serrors.WrapStr("reading row", err)
		}
		seg, err := beacon.UnpackBeaconPB(rawBeacon)

		if err != nil {
			return [][]byte{}, []*cppb.IRECBeaconUnopt{}, serrors.WrapStr("parsing beacon", err)
		}
		res = append(res, &cppb.IRECBeaconUnopt{
			PathSeg: seg,
			InIfId:  uint32(InIntfID),
			Id:      count,
		})
		fbs = append(fbs, flatbuffer)
		count += 1
	}

	if err := rows.Err(); err != nil {
		log.Error("err when selecting beacons", "err", err)
		return [][]byte{}, []*cppb.IRECBeaconUnopt{}, err
	}
	return fbs, res, nil
}
func (e *executor) buildQuery(ctx context.Context, opts *beacon2.QueryOptions) (string, []interface{}) {
	var args []interface{}
	query := "SELECT DISTINCT b.RowID, b.LastUpdated, b.InIntfID, b.Usage, b.Flatbuffer, b.Beacon FROM Beacons b"
	if len(opts.Labels) > 0 {
		query += ", Labels l"
	}
	where := []string{}

	if len(opts.Algorithms) > 0 {
		subQ := make([]string, 0, len(opts.Algorithms))
		for _, alg := range opts.Algorithms {
			query = "b.AlgorithmHash=?"
			args = append(args, alg)
			if len(alg.AlgId) > 0 {
				subQ2 := make([]string, 0, len(alg.AlgId))
				for _, algId := range alg.AlgId {
					subQ2 = append(subQ2, "b.AlgorithmId=?")
					args = append(args, algId)
				}
				query += fmt.Sprintf(" AND (%s)", strings.Join(subQ2, " OR "))
			}

			// (AlgorithmHash = ? AND (algorithmId = ? OR algorithmId = ?))
			subQ = append(subQ, fmt.Sprintf("(%s)", query))
		}
		where = append(where, fmt.Sprintf("(%s)", strings.Join(subQ, " OR ")))
	}
	if len(opts.Origins) > 0 {
		subQ := make([]string, 0, len(opts.Origins))
		for _, origin := range opts.Origins {
			query = ""
			switch {
			case origin.OriginAS.IsZero():
				continue
			case origin.OriginAS.ISD() == 0:
				query += "b.StartAs=?"
				args = append(args, origin.OriginAS.AS())
			case origin.OriginAS.AS() == 0:
				query += "b.StartIsd=?"
				args = append(args, origin.OriginAS.ISD())
			case origin.OriginAS.ISD() != 0 && origin.OriginAS.AS() != 0:
				query += "(b.StartIsd=? AND b.StartAs=?)"
				args = append(args, origin.OriginAS.ISD(), origin.OriginAS.AS())
			}
			if len(origin.OriginIntfGroup) > 0 {
				subQ2 := make([]string, 0, len(origin.OriginIntfGroup))
				for _, intfGroup := range origin.OriginIntfGroup {
					subQ2 = append(subQ2, "b.StartIntfGroup=?")
					args = append(args, intfGroup)
				}
				query += fmt.Sprintf(" AND (%s)", strings.Join(subQ2, " OR "))
			}

			// (AlgorithmHash = ? AND (algorithmId = ? OR algorithmId = ?))
			subQ = append(subQ, fmt.Sprintf("(%s)", query))
		}
		where = append(where, fmt.Sprintf("(%s)", strings.Join(subQ, " OR ")))
	}

	if len(opts.Labels) > 0 {
		subQ := make([]string, 0, len(opts.Origins))
		for _, label := range opts.Labels {

			subQ = append(subQ, "l.Label=?")
			args = append(args, label)
			// (AlgorithmHash = ? AND (algorithmId = ? OR algorithmId = ?))

		}
		where = append(subQ, fmt.Sprintf("l.FullId = b.FullId AND (%s)", strings.Join(subQ, " OR ")))
	}
	if opts.OnlyUnmarked {
		where = append(where, "(FetchStatus=0)")
	}
	if len(where) > 0 {
		query += "\n" + fmt.Sprintf("WHERE %s", strings.Join(where, " AND\n"))
	}

	if opts.Maximum > 0 {
		query += "\n LIMIT ?"
		args = append(args, opts.Maximum)
	}
	query += "\n" + "ORDER BY LastUpdated DESC"
	return query, args
}

func insertNewBeacon(
	ctx context.Context,
	tx *sql.Tx,
	b beacon.Beacon,
	usage beacon.Usage,
	now time.Time,
) error {

	segID := b.Segment.ID()
	fullID := b.Segment.IRECID()
	packed, err := beacon.PackBeacon(b.Segment)
	if err != nil {
		return db.NewInputDataError("pack segment", err)
	}
	packedFlatBuffer, err := beacon.PackBeaconFB(&b)
	if err != nil {
		return db.NewInputDataError("pack segment", err)
	}
	start := b.Segment.FirstIA()
	infoTime := b.Segment.Info.Timestamp.Unix()
	lastUpdated := now.UnixNano()
	expTime := b.Segment.MaxExpiry().Unix()
	intfGroup := uint16(0)
	algorithmHash := []byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9} // Fallback RAC.
	algorithmId := uint32(0)
	pullBased := false
	pullBasedMinBeacons := uint32(0)  // todo(jvb): lower limit must be able to be set by AS owner.
	pullBasedPeriod := now.UnixNano() // todo(jvb): Upper limit must be able to be set by AS owner
	pullBasedHyperPeriod := now.UnixNano()
	pullBasedTargetAs := 0
	pullBasedTargetIsd := 0
	if b.Segment.ASEntries[0].Extensions.Irec != nil {
		intfGroup = b.Segment.ASEntries[0].Extensions.Irec.InterfaceGroup
		algorithmHash = b.Segment.ASEntries[0].Extensions.Irec.AlgorithmHash
		algorithmId = b.Segment.ASEntries[0].Extensions.Irec.AlgorithmId
		pullBased = b.Segment.ASEntries[0].Extensions.Irec.PullBased
		pullBasedMinBeacons = b.Segment.ASEntries[0].Extensions.Irec.PullBasedMinBeacons
		pullBasedPeriod = now.Add(b.Segment.ASEntries[0].Extensions.Irec.PullBasedPeriod).UnixNano()
		pullBasedHyperPeriod = now.Add(b.Segment.ASEntries[0].Extensions.Irec.PullBasedHyperPeriod).UnixNano()
		if !b.Segment.ASEntries[0].Extensions.Irec.PullBasedTarget.IsZero() {

			pullBasedTargetAs = int(b.Segment.ASEntries[0].Extensions.Irec.PullBasedTarget.AS())
			pullBasedTargetIsd = int(b.Segment.ASEntries[0].Extensions.Irec.PullBasedTarget.ISD())
		}
	}
	inst := `
	INSERT INTO Beacons (SegID, FullID, StartIsd, StartAs, StartIntfGroup, AlgorithmHash, AlgorithmId, InIntfID, HopsLength, InfoTime,
		ExpirationTime, LastUpdated, Usage, Beacon, Flatbuffer, FetchStatus, FetchStatusExpirationTime, PullBased, PullBasedMinBeacons, PullBasedPeriod, PullBasedHyperPeriod, PullBasedTargetAs, PullBasedTargetIsd)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?)
	`

	_, err = tx.ExecContext(ctx, inst, segID, fullID, start.ISD(), start.AS(),
		intfGroup,
		algorithmHash,
		algorithmId, b.InIfId,
		len(b.Segment.ASEntries), infoTime, expTime, lastUpdated, usage, packed, packedFlatBuffer, pullBased, pullBasedMinBeacons, pullBasedPeriod, pullBasedHyperPeriod, pullBasedTargetAs, pullBasedTargetIsd)
	if err != nil {
		return db.NewWriteError("insert beacon", err)
	}
	return nil
}
