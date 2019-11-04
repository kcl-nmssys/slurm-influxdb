# Slurm to InfluxDB stats collection script

This script will collect various statistics from Slurm and insert them into an InfluxDB time-series database.

The following metrics are currently collected:

* CPU core usage across entire cluster
* CPU core total across entire cluster
* CPU core usage by group (list of groups configurable)
* CPU core usage by user
* CPU core usage by partition
* CPU core total by partition

* GPU usage across entire cluster
* GPU total across entire cluster
* GPU usage by group (list of groups configurable)
* GPU usage by user
* GPU usage by partition
* GPU total by partition

* Memory usage across entire cluster
* Memory total across entire cluster
* Memory usage by group (list of groups configurable)
* Memory usage by user
* Memory usage by partition
* Memory total by partition

* Queue time total across the entire cluster
* Queue time total per group (list of groups configurable)
* Queue time total per user
* Queue time total per partition

* Running jobs for whole cluster
* Running jobs by group (list of groups configurable)
* Running jobs by user
* Running jobs by partition

* Pending jobs for whole cluster
* Pending jobs by group (list of groups configurable)
* Pending jobs by user
* Pending jobs by partition
