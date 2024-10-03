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

const (
	// SchemaVersion is the version of the SQLite schema understood by this backend.
	// Whenever changes to the schema are made, this version number should be increased
	// to prevent data corruption between incompatible database schemas.
	SchemaVersion = 1
	// Schema is the SQLite database layout.
	// fetchstatuses; 0 = unfetched, 1 = fetched, but not processed, 2 = processed
	Schema = `CREATE TABLE Beacons(
		RowID INTEGER PRIMARY KEY,
		SegID DATA NOT NULL,
		FullID DATA UNIQUE NOT NULL,
		StartIsd INTEGER NOT NULL,
		StartAs INTEGER NOT NULL,
		StartIntfGroup INTEGER NOT NULL,
		AlgorithmHash DATA NOT NULL,
		AlgorithmId DATA NOT NULL,
		InIntfID INTEGER NOT NULL,
		HopsLength INTEGER NOT NULL,
		InfoTime INTEGER NOT NULL,
		ExpirationTime INTEGER NOT NULL,
		LastUpdated INTEGER NOT NULL,
		Usage INTEGER NOT NULL,
		FetchStatus INTEGER NOT NULL,
		FetchStatusExpirationTime INTEGER NOT NULL,
		PullBased BOOL NOT NULL,
		PullBasedTargetIsd INTEGER NOT NULL,
		PullBasedTargetAs INTEGER NOT NULL,
		PullBasedPeriod INTEGER Not NULL,
		PullBasedHyperPeriod INTEGER Not NULL,
		PullBasedMinBeacons INTEGER Not NULL,
		Beacon BLOB NOT NULL,
		Flatbuffer BLOB
	);


CREATE TABLE Algorithm(
		RowID INTEGER PRIMARY KEY,
		AlgorithmHash DATA UNIQUE NOT NULL,
		AlgorithmCode BLOB NOT NULL,
		AddedTime INTEGER NOT NULL
	);


CREATE TABLE Labels(
    FullID DATA NOT NULL REFERENCES Beacons(FullID) ON DELETE CASCADE,
    Label INTEGER NOT NULL,
    PRIMARY KEY (FullID, Label)
);

CREATE TABLE RacJobs(
	StartIsd INTEGER NOT NULL,
	StartAs INTEGER NOT NULL,
	StartIntfGroup INTEGER NOT NULL,
	AlgorithmHash DATA NOT NULL,
	AlgorithmId DATA NOT NULL,
	PullBased BOOL NOT NULL,
	PullBasedTargetIsd INTEGER NOT NULL,
	PullBasedTargetAs INTEGER NOT NULL,
	Valid BOOL NOT NULL,
	LastExecuted INTEGER NOT NULL,
	PRIMARY KEY (StartIsd, StartAs, StartIntfGroup, AlgorithmHash, AlgorithmId, PullBased, PullBasedTargetIsd, PullBasedTargetAs)
)

CREATE INDEX BeaconJobIndex ON Beacons(StartIsd, StartAs, StartIntfGroup, AlgorithmHash, AlgorithmId, PullBased, PullBasedTargetIsd, PullBasedTargetAs);
CREATE INDEX RacJobsIndex ON RacJobs(StartIsd, StartAs, StartIntfGroup, AlgorithmHash, AlgorithmId, PullBased, PullBasedTargetIsd, PullBasedTargetAs);
CREATE INDEX AlgorithmIndex ON Algorithm(AlgorithmHash);
	`
	// marker 0 = new
	// marker 1 = already processed
	// Algorithm statuses:
	// 0: Unfetched
	// 1: Fetching -> check FetchTime whether expired ?
	// 2: Installed
	// 3: Manually installed (e.g. from disk)
	BeaconsTable = "Beacons"
)
