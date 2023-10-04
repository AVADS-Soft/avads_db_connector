package avads_db_connector

import (
	"strings"
	"testing"
	"time"
)

func TestTCPBaseManipulation(t *testing.T) {
	conn := NewConnection("127.0.0.1", "7777", "admin", "admin")
	tm, _ := time.ParseDuration("500s")
	conn.TimeOut = tm
	conn.Reconnect.ReConnectTime = time.Second * 2
	errConnect := conn.Connect()
	if errConnect != nil {
		t.Fatal(errConnect)
	}

	bl, errGetBaseList := conn.GetBaseList()
	if errGetBaseList == nil {
		for _, val := range *bl {
			if strings.HasPrefix(val.Name, "test tcp api") {
				errRemove := conn.RemoveBase(val.Name)
				if errRemove != nil {
					t.Fatal(errRemove)
				}
			}
		}
	} else {
		t.Fatal(errGetBaseList)
	}

	baseInst := BaseT{
		Name:    "test tcp api",
		Comment: "test tcp api",
		Path:    "./db/test_tcp_api",
		Looping: LoopingT{
			Type: 0,
			Lt:   "",
		},
		DbSize:        "100m",
		FsType:        FS_FS,
		AutoAddSeries: true,
		AutoSave:      false,
	}

	errAddBase := conn.AddBase(baseInst)
	if errAddBase != nil {
		t.Fatal(errAddBase)
	}

	oldName := baseInst.Name
	baseInst.Name = "test tcp api changed"
	baseInst.Comment = baseInst.Name
	errUpdateBase := conn.UpdateBase(oldName, baseInst)
	if errUpdateBase != nil {
		t.Fatal(errUpdateBase)
	}

	getBaseInst, errGetBase := conn.GetBase(baseInst.Name)
	if errGetBase != nil {
		t.Fatal(errGetBase)
	}

	if getBaseInst.Name != baseInst.Name {
		t.Fatal("base not equal")
	}
}
