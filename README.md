![ORIGINIUM](./images/ORIGINIUM.png)

LSM-Tree based storage engine used by FOIVER system.

## Install

```shell
go get -u github.com/B1NARY-GR0UP/originium
```

## Usage

- **Open**

```go
// use originium.Config to customize the db behavior
db, err := originium.Open("your-dir", originium.DefaultConfig)
```

- **Set**

```go
db.Set("hello", []byte("originium"))
``` 

- **Get**
 
```go
v, ok := db.Get("hello")
```

- **Delete**

```go
db.Delete("hello")
```

- **Close**

```go
db.Close()
```

## License

ORIGINIUM is distributed under the [Apache License 2.0](./LICENSE). The licenses of third party dependencies of ORIGINIUM are explained [here](./licenses).

## ECOLOGY

<p align="center">
<img src="https://github.com/justlorain/justlorain/blob/main/images/PROJECT-FOIVER.png" alt="PROJECT: FOIVER"/>
<br/><br/>
ORIGINIUM is Part of <a href="https://github.com/B1NARY-GR0UP">PROJECT: FOIVER</a>
</p>
