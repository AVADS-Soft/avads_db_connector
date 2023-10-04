# avads_db_connector

Библиотека на языке Go для подключения и управления данными базы занных ***AVADS TSDB*** (сервер архивирования)

# Установка
Используй `go get` для установки библиотеки
```bash
go get -u github.com/AVADS-Soft/avads_db_connector
```

# Пример использования

```go
package main

import (
	"lite_db_connector"
)

func main() {
    onn := avads_db_connectror.NewConnection("127.0.0.1", "7777","admin", "admin")
    errConnect := conn.Connect()
	if errConnect != nil {
		t.Fatal(errConnect)
	}

	bl, errGetBaseList := conn.GetBaseList()
    if errGetBaseList!=nil {
		t.Fatal(errGetBaseList)
    }

    for _, val := range *bl {
        ...
    }

    errAddBase := conn.AddBase(BaseT{
		Name:    "test tcp api",
		Comment: "test tcp api",
		Path:    "./db/test_tcp_api",
		Looping: avads_db_connector.LoopingT{
			Type: 0,
			Lt:   "",
		},
		DbSize:        "100m",
		FsType:        avads_db_connector.FS_FS,
		AutoAddSeries: true,
		AutoSave:      false,
	})
	if errAddBase != nil {
		log.Fatal(errAddBase)
	}

    errAddSer := conn.AddSeries("test tcp api", SeriesT{
		Name:        "LINT_single",
		Type:        types.LINT,
		Id:          0,
		Comment:     "",
		ViewTimeMod: 0,
		Looping:     avads_db_connector.LoopingT{},
		Class:       0,
	})
	if errAddSer != nil {
		log.Fatal(errAddSer)
	}

    baseOpenId := 1

    errOpen := conn.OpenBase(baseOpenId, "test tcp api")
	if errOpen != nil {
		log.Fatal(errOpen)
	}

    for i := 0; i < 10; i++ {
		errAdd := conn.DataAddRow(baseOpenId, 2, 0, time.Now().Unix(), 92, i)
		if errAdd != nil {
			t.Fatal(errAdd)
		}
	}

	errClose := conn.CloseBase(baseOpenId)
	if errClose != nil {
		log.Fatal(errClose)
	}
}
```
