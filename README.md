# kvnDB - Key-Value Non-DataBase
kvnDB (pronounced as "Kevin Dee Bee") is not a database. It is a lame embedded in-memory key-value storage with on-demand snapshots
and some sort of history keeping.

## Features
* Fast operations. All data is stored in memory. Watch out for OOM
* Thread/goroutine safe. All operations are done via mutex, so reading and writing data can be done from different routines (but watch out for concurrency)
* Simple API. Very simple
* On-demand persistence. Via API data snapshot can be written to disk. Watch for disk space/IO as data written uncompressed
* Snapshot history. Can maintain desired history of snapshots. See API for `Save()`

## Why
* When need simple and fast data storage
* When need that storage also be persisted to disk

## Usage
Get it:
```shell
$ go get github.com/akamensky/kvndb
```
See documentation for API
