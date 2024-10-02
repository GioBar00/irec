// Copyright 2019 Anapaya Systems
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
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
	"sync"
	"time"

	"github.com/scionproto/scion/control/irec/egress/storage"

	_ "github.com/mattn/go-sqlite3"

	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/private/serrors"
	"github.com/scionproto/scion/private/storage/db"
)

//var _ beacon.DB = (*Backend)(nil)

type Backend struct {
	db *sql.DB
	*executor
}

// New returns a new SQLite backend opening a database at the given path. If
// no database exists a new database is be created. If the schema version of the
// stored database is different from the one in schema.go, an error is returned.
func New(path string, ia addr.IA) (*Backend, error) {
	db, err := db.NewSqlite(path, Schema, SchemaVersion)
	if err != nil {
		return nil, err
	}
	return &Backend{
		executor: &executor{
			db: db,
			ia: ia,
		},
		db: db,
	}, nil
}

// SetMaxOpenConns sets the maximum number of open connections.
func (b *Backend) SetMaxOpenConns(maxOpenConns int) {
	b.db.SetMaxOpenConns(maxOpenConns)
}

// SetMaxIdleConns sets the maximum number of idle connections.
func (b *Backend) SetMaxIdleConns(maxIdleConns int) {
	b.db.SetMaxIdleConns(maxIdleConns)
}

// Close closes the database.
func (b *Backend) Close() error {
	return b.db.Close()
}

type executor struct {
	sync.RWMutex
	db db.Sqler
	ia addr.IA
}

func (e *executor) BeaconsThatShouldBePropagated(ctx context.Context, beacons []storage.EgressBeacon, expiry time.Time) ([]storage.EgressBeacon, error) {
	e.Lock()
	defer e.Unlock()
	var query string
	tx, err := e.db.(*sql.DB).BeginTx(ctx, nil)
	if err != nil {
		return []storage.EgressBeacon{}, serrors.Join(db.NewWriteError("starting transaction", err), tx.Rollback())
	}
	// create temporary table to store the beacons
	_, err = tx.ExecContext(ctx, `CREATE TEMPORARY TABLE BeaconsToFilter (BeaconHash DATA, EgressIntf INTEGER, Idx INTEGER)`)
	if err != nil {
		return nil, serrors.Join(db.NewWriteError("creating temporary table BTF", err), tx.Rollback())
	}
	for _, b := range beacons {
		for _, intf := range b.EgressIntfs {
			_, err = tx.ExecContext(ctx, `INSERT INTO BeaconsToFilter (BeaconHash, EgressIntf, Idx) VALUES (?, ?, ?)`,
				*b.BeaconHash, intf, b.Index)
			if err != nil {
				return []storage.EgressBeacon{}, serrors.Join(db.NewWriteError("inserting beacon hash", err), tx.Rollback())
			}
		}
	}
	// create the table to store the beacons that should be propagated
	_, err = tx.ExecContext(ctx, `CREATE TEMPORARY TABLE BeaconsToPropagate (BeaconHash DATA, EgressIntf INTEGER, Idx INTEGER)`)
	if err != nil {
		return []storage.EgressBeacon{}, serrors.Join(db.NewWriteError("creating temporary table BTP", err), tx.Rollback())
	}
	// select the beacons that should be propagated by outer join with the beacons table
	query = `INSERT INTO BeaconsToPropagate (BeaconHash, EgressIntf, Idx)
	SELECT DISTINCT BTF.BeaconHash, BTF.EgressIntf, BTF.Idx
	FROM BeaconsToFilter AS BTF
	WHERE NOT EXISTS (
		SELECT 1
		FROM Beacons AS B
		WHERE B.BeaconHash = BTF.BeaconHash AND B.EgressIntf = BTF.EgressIntf
	)
	ORDER BY BTF.Idx
	`
	_, err = tx.ExecContext(ctx, query)
	if err != nil {
		return []storage.EgressBeacon{}, serrors.Join(db.NewWriteError("filtering beacons to propagate", err), tx.Rollback())
	}

	query = `SELECT BeaconHash, EgressIntf, Idx FROM BeaconsToPropagate`
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return []storage.EgressBeacon{}, serrors.Join(db.NewReadError("reading beacons to propagate", err), tx.Rollback())
	}
	defer rows.Close()
	var res []storage.EgressBeacon
	var currIdx = -1
	var b storage.EgressBeacon
	for rows.Next() {
		var beaconHash []byte
		var intf int
		var index int
		if err := rows.Scan(&beaconHash, &intf, &index); err != nil {
			return []storage.EgressBeacon{}, err
		}
		if currIdx >= 0 && res[currIdx].Index == index {
			res[currIdx].EgressIntfs = append(res[currIdx].EgressIntfs, uint32(intf))
		} else {
			b = storage.EgressBeacon{
				BeaconHash:  &beaconHash,
				EgressIntfs: []uint32{uint32(intf)},
				Index:       index,
			}
			res = append(res, b)
			currIdx++
		}
	}

	if currIdx >= 0 {
		// insert the beacons that should be propagated into the beacons table
		query = `INSERT INTO Beacons (BeaconHash, EgressIntf, ExpirationTime)
					SELECT BeaconHash, EgressIntf, ?1 FROM BeaconsToPropagate`
		_, err = tx.ExecContext(ctx, query, expiry.Unix())
		if err != nil {
			return []storage.EgressBeacon{}, serrors.Join(db.NewWriteError("inserting beacons to propagate", err), tx.Rollback())
		}
	}

	// delete the temporary table
	_, err = tx.ExecContext(ctx, `DROP TABLE BeaconsToPropagate`)
	if err != nil {
		return []storage.EgressBeacon{}, serrors.Join(db.NewWriteError("dropping temporary table BTP", err), tx.Rollback())
	}
	_, err = tx.ExecContext(ctx, `DROP TABLE BeaconsToFilter`)
	if err != nil {
		return []storage.EgressBeacon{}, serrors.Join(db.NewWriteError("dropping temporary table BTF", err), tx.Rollback())
	}

	if err := tx.Commit(); err != nil {
		return []storage.EgressBeacon{}, serrors.Join(db.NewWriteError("committing transaction", err), tx.Rollback())
	}

	return res, nil
}

func (e *executor) IsBeaconAlreadyPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface) (bool, int, error) {
	e.Lock()
	defer e.Unlock()
	return e.isBeaconAlreadyPropagated(ctx, beaconHash, intf)
}

func (e *executor) isBeaconAlreadyPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface) (bool, int, error) {
	rowID := 0
	query := "SELECT RowID FROM Beacons WHERE BeaconHash=? AND EgressIntf=?"

	rows, err := e.db.QueryContext(ctx, query, beaconHash, intf.TopoInfo().ID)
	// Beacon hash is not in the table.
	//log.Debug("isBeaconAlreadyPropagated", "err", rowID)
	if err != nil {
		return false, -1, db.NewReadError("Failed to lookup beacon hash", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return false, -1, nil
	}
	if err = rows.Scan(&rowID); err != nil {
		return false, -1, err
	}
	return true, rowID, nil
}

func (e *executor) UpdateExpiry(ctx context.Context, beaconHash []byte, intf *ifstate.Interface, expiry time.Time) error {
	e.Lock()
	defer e.Unlock()

	query := "UPDATE Beacons SET ExpirationTime=?1 WHERE RowID IN (SELECT RowID FROM Beacons WHERE BeaconHash=?2 AND EgressIntf=?3 ORDER BY ExpirationTime DESC LIMIT 1)"
	_, err := e.db.ExecContext(ctx, query, expiry.Unix(), beaconHash, intf.TopoInfo().ID)
	if err != nil {
		return db.NewWriteError("updating beacon hash expiry", err)
	}
	// nRows, err := queryRes.RowsAffected()
	// if err != nil {
	// 	return db.NewWriteError("getting rows affected", err)
	// }
	// log.Debug("DB; UpdateExpiry; Update Beacons", "rows affected", nRows)
	return nil
}

// updateExistingBeacon updates the changeable data for an existing beacon.
func (e *executor) updateExpiry(ctx context.Context, rowID int, expiry time.Time) error {
	inst := `UPDATE Beacons SET ExpirationTime=? WHERE RowID=?`
	_, err := e.db.ExecContext(ctx, inst, expiry.Unix(), rowID)
	if err != nil {
		return db.NewWriteError("updating beacon hash expiry", err)
	}
	return nil
}

func insertNewBeaconHash(
	ctx context.Context,
	tx *sql.Tx,
	beaconHash []byte, intf *ifstate.Interface, expiry time.Time,
) error {

	inst := `
	INSERT INTO Beacons (BeaconHash, EgressIntf, ExpirationTime)
	VALUES (?, ?, ?)
	`

	_, err := tx.ExecContext(ctx, inst, beaconHash, intf.TopoInfo().ID, expiry.Unix())
	if err != nil {
		return db.NewWriteError("insert beaconhash", err)
	}
	return nil
}

func (e *executor) GetDBSize(ctx context.Context) (int, error) {
	e.RLock()
	defer e.RUnlock()
	// Get the size of the database with select count(*)
	query := "SELECT count(*) FROM Beacons"
	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return 0, db.NewReadError("Failed to get database size", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, db.NewReadError("Failed to get database size", nil)
	}
	var size int
	if err = rows.Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}

func (e *executor) MarkBeaconAsPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface, expiry time.Time) error {
	e.Lock()
	defer e.Unlock()
	// Insert new beacon.
	err := db.DoInTx(ctx, e.db, func(ctx context.Context, tx *sql.Tx) error {
		return insertNewBeaconHash(ctx, tx, beaconHash, intf, expiry)
	})
	return err

}

func (e *executor) DeleteExpiredBeacons(ctx context.Context, now time.Time) (int, error) {
	return e.deleteInTx(ctx, func(tx *sql.Tx) (sql.Result, error) {
		delStmt := `DELETE FROM Beacons WHERE ExpirationTime < ?`
		return tx.ExecContext(ctx, delStmt, now.Unix())
	})
}

func (e *executor) deleteInTx(
	ctx context.Context,
	delFunc func(tx *sql.Tx) (sql.Result, error),
) (int, error) {

	e.Lock()
	defer e.Unlock()
	return db.DeleteInTx(ctx, e.db, delFunc)
}
