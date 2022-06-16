# benchio

[![Go Report Card](https://goreportcard.com/badge/github.com/giacomoguiulfo/benchio)](https://goreportcard.com/report/github.com/giacomoguiulfo/benchio)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/github.com/giacomoguiulfo/benchio)
[![Release](https://img.shields.io/github/release/giacomoguiulfo/benchio.svg?style=flat-square)](https://github.com/giacomoguiulfo/benchio/releases/latest)

Benchmarking tool for S3 compatible object storage servers

# Getting Started

## Requirements

 - golang >=1.9

## Installation

```sh
go get -u https://github.com/giacomoguiulfo/benchio.git
```

## Simple usage

```
benchio run -f benchio.yaml
```
> This assumes that the `GOBIN` environmental variable (`GOPATH/bin`) is in your `PATH`.

## Advanced Usage

Options can be used as command line arguments or stored in a configuration file
which supports YAML, JSON, or TOML formats.

The default benchio.yaml file contains all the necessary values to run a basic
read/write/cleanup workload.

```yaml
accessKey: MZZRYNUQKEJZUTKLLHAD
secretKey: hRwZ5GA7VUhh=vLdBLUqZuRqcryqyVHhuCopR5a4
endpoint: http://localhost:8000
bucket: testbucket
objectSize: 1024
multipartSize: 0
objectNamePrefix: testobject
numClients: 10
numSamples: 100
verbose: false
region: us-east-1
cleanup: true
write: true
read: true
```

## Available Parameters

| Parameter                           | Description                                                                                                      |
| ----------------------------------- | -----------------------------------------------------------------------------------------------------------------|
| `accessKey`                         | This is your user access key to access given buckets                                                             |
| `secretKey`                         | This is the secret key used to access given buckets                                                              |
| `endpoint`                          | These are endpoint targets for the workloads. Multiple can be passed as a comma separated list.                  |
| `bucket`                            | The target bucket to be used. Must already be created on the specified endpoint                                  |
| `objectSize`                        | Size for each object to be used in the workload (Currently measured in bytes)                                    |
| `multipartSize`                     | Use a multipart transfer, with parts of the given size (in bytes). Use 0 (the default) to disable                |
| `objectNamePrefix`                  | Prefix to be used in the object naming                                                                           |
| `numClients`                        | Number of clients, also referred to as 'workers'                                                                 |
| `numSamples`                        | Number of objects to server to the endpoint                                                                      |
| `verbose`                           | Gives more information on each object operation                                                                  |
| `region`                            | The AWS compatible region where your bucket is located                                                           |
| `write`                             | A bool to enable/disable writes, typically this will be the first operation to run                               |
| `read`                              | A bool to enable/disable reads, runs after writes have completed (currently buggy if using only read operations) |
| `cleanup`                           | A bool to enable/disable cleanup operations after successful read/writes                                         |
