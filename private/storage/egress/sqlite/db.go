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

	_ "github.com/mattn/go-sqlite3"

	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/pkg/addr"
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

func (e *executor) IsBeaconAlreadyPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface) (bool, int, error) {
	rowID := 0
	query := "SELECT RowID FROM Beacons WHERE BeaconHash=? AND EgressIntf=?"

	err := e.db.QueryRowContext(ctx, query, beaconHash, intf.TopoInfo().ID).Scan(&rowID)
	// Beacon hash is not in the table.
	//log.Debug("isBeaconAlreadyPropagated", "err", rowID)
	if err == sql.ErrNoRows {
		return false, -1, nil
	}
	if err != nil {
		return false, -1, db.NewReadError("Failed to lookup beacon hash", err)
	}
	return true, rowID, nil
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

func (e *executor) MarkBeaconAsPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface, expiry time.Time) error {
	e.Lock()
	defer e.Unlock()
	propStatus, rowID, err := e.IsBeaconAlreadyPropagated(ctx, beaconHash, intf)
	if err != nil {
		return err
	}
	if propStatus {
		// Update expiry
		return e.updateExpiry(ctx, rowID, expiry)
	}
	// Insert new beacon.
	err = db.DoInTx(ctx, e.db, func(ctx context.Context, tx *sql.Tx) error {
		return insertNewBeaconHash(ctx, tx, beaconHash, intf, expiry)
	})
	if err != nil {
		return err
	}
	return nil

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
