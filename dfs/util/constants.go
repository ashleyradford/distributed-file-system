package util

const HB_CHECK = 5   // how often controller checks if nodes are still alive
const RATE = 5       // how often storage node sends a heartbeat
const MISSED = 3     // how many heartbeats missed before node is dead
const REPLICAS = 3   // how many total copies of a chunk
const REP_CHECK = 12 // how often controller checks that replication level is maintained
const TIMEOUT = 15   // how long we give all replicas to be stored
