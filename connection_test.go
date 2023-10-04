package avads_db_connector

import (
	"log"
	"testing"
	"time"

	"avads_db_connector/types"
)

func TestTCPFails(t *testing.T) {

	connB := NewConnection("1", "7777", "admin", "admin")
	tm, _ := time.ParseDuration("5s")
	connB.TimeOut = tm
	errConnectB := connB.Connect()
	if errConnectB == nil {
		t.Fatal("invalid connection reaction")
	}

	conn := NewConnection("127.0.0.1", "7777", "adminz", "adminz")

	conn.TimeOut = tm
	errConnect := conn.Connect()
	if errConnect == nil {
		t.Fatal("invalid connection reaction")
	}

	conn = NewConnection("127.0.0.1", "7777", "admin", "admin")

	conn.TimeOut = tm
	errConnect = conn.Connect()
	if errConnect != nil {
		t.Fatal(errConnect)
	}

	//----------------------

	errOpenBaseSaw := conn.OpenBase(212, "none base")
	if errOpenBaseSaw == nil || errOpenBaseSaw.Error() != "#11002 incorrect base name" {
		t.Fatal("OpenBase incorrect answer")
	}

	b, errGetBSaw := conn.DataGetBoundary(212, 1)
	if errGetBSaw == nil {
		t.Fatal("DataGetBoundary incorrect answer")
	}
	_ = b

	rec, errGetRecSaw := conn.DataGetValueAtTime(212, 1, 2, 0)
	if errGetRecSaw == nil {
		t.Fatal("DataGetValueAtTime incorrect answer")
	}
	_ = rec

	recRange, errGetRecRangeSaw := conn.DataGetRangeDirection(222, 1, types.SimpleClass, 1, 100, 0, 10, 0)
	if errGetRecRangeSaw == nil {
		t.Fatal("DataGetRangeDirection incorrect answer")
	}
	_ = recRange

	//----------------------

	errCloseAll := conn.Close()
	if errCloseAll != nil {
		t.Fatal(errCloseAll)
	}
}

func TestTCPMain(t *testing.T) {
	conn := NewConnection("127.0.0.1", "7777", "admin", "admin")
	tm, _ := time.ParseDuration("5s")
	conn.TimeOut = tm
	errConnect := conn.Connect()
	if errConnect != nil {
		t.Fatal(errConnect)
	}

	bl, errGetBaseList := conn.GetBaseList()
	if errGetBaseList == nil {
		for _, val := range *bl {
			if val.Name == "test tcp api" {
				errRemove := conn.RemoveBase("test tcp api")
				if errRemove != nil {
					t.Fatal(errRemove)
				}
				break
			}
		}
	} else {
		t.Fatal(errGetBaseList)
	}

	_, errGetBase := conn.GetBase("test tcp api")
	if errGetBase == nil {
		errRemove := conn.RemoveBase("test tcp api")
		if errRemove != nil {
			t.Fatal(errRemove)
		}
	}

	errAddBase := conn.AddBase(BaseT{
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
	})
	if errAddBase != nil {
		t.Fatal(errAddBase)
	}

	errAddSer := conn.AddSeries("test tcp api", SeriesT{
		Name:        "LINT_single",
		Type:        types.LINT,
		Id:          0,
		Comment:     "",
		ViewTimeMod: 0,
		Looping:     LoopingT{},
		Class:       0,
	})
	if errAddSer != nil {
		t.Fatal(errAddSer)
	}

	errAddSer = conn.AddSeries("test tcp api", SeriesT{
		Name:        "LINT_cache",
		Type:        types.LINT,
		Id:          1,
		Comment:     "",
		ViewTimeMod: 0,
		Looping:     LoopingT{},
		Class:       0,
	})
	if errAddSer != nil {
		t.Fatal(errAddSer)
	}

	errAddSer = conn.AddSeries("test tcp api", SeriesT{
		Name:        "LINT_multi",
		Type:        types.LINT,
		Id:          2,
		Comment:     "",
		ViewTimeMod: 0,
		Looping:     LoopingT{},
		Class:       0,
	})
	if errAddSer != nil {
		t.Fatal(errAddSer)
	}

	sers, errGetSer := conn.GetAllSeries("test tcp api")
	if errGetSer != nil {
		t.Fatal(errGetSer)
	}

	log.Println(sers)

	errOpen2 := conn.OpenBase(2, "test tcp api")
	if errOpen2 != nil {
		t.Fatal(errOpen2)
	}

	errClose := conn.CloseBase(2)
	if errClose != nil {
		t.Fatal(errClose)
	}

	errOpen := conn.OpenBase(222, "test tcp api")
	if errOpen != nil {
		t.Fatal(errOpen)
	}

	for i := 0; i < 10; i++ {
		errAdd := conn.DataAddRow(222, 0, 0, int64(i), 92, i)
		if errAdd != nil {
			t.Fatal(errAdd)
		}
	}

	rowsForCache := conn.NewRows()
	for i := 0; i < 10; i++ {
		errAdd := rowsForCache.DataAddRow(1, 0, int64(i), 92, i)
		if errAdd != nil {
			t.Fatal(errAdd)
		}
	}
	count, errAddCache := conn.DataAddRowCache(222, rowsForCache)
	if errAddCache != nil {
		t.Fatal(errAddCache)
	}
	if count != len(rowsForCache.cache)+8 {
		t.Fatal("incorrect count size")
	}

	rowsForCache = conn.NewRows()
	for i := 0; i < 10; i++ {
		errAdd := rowsForCache.DataAddRow(2, 0, int64(i), 92, i)
		if errAdd != nil {
			t.Fatal(errAdd)
		}
	}
	errAddRows := conn.DataAddRows(222, rowsForCache)
	if errAddRows != nil {
		t.Fatal(errAddRows)
	}

	b, errGetB := conn.DataGetBoundary(222, 2)
	if errGetB != nil {
		t.Fatal(errGetB)
	}
	log.Println(b)

	dataMath, errDataMath := conn.DataMathFunc(222, 1, b.Min, b.Max, 3)
	if errDataMath != nil {
		t.Fatal(errDataMath)
	}
	log.Println(dataMath)

	rec, errGetRec := conn.DataGetValueAtTime(222, 1, 2, 0)
	if errGetRec != nil {
		t.Fatal(errGetRec)
	}
	log.Println(rec)

	recRange, errGetRecRange := conn.DataGetRangeDirection(222, 1, types.SimpleClass, 1, 100, 0, 10, 0)
	if errGetRecRange != nil {
		t.Fatal(errGetRecRange)
	}
	log.Println(recRange)
	errDel := conn.DataDeleteRow(222, 2, 2)
	if errDel != nil {
		t.Fatal(errDel)
	}

	log.Println(recRange)
	c, errDelS := conn.DataDeleteRows(222, 2, 5, 7)
	if errDelS != nil {
		t.Fatal(errDelS)
	}
	if c != 3 {
		t.Fatal("incorrect del count")
	}

	cp, errGetCP := conn.DataGetCP(222, 2, 4)
	if errGetCP != nil {
		t.Fatal(errGetCP)
	}

	recs, errFromCP := conn.DataGetFromCP(222, cp, 1, 2, types.SimpleClass)
	if errFromCP != nil {
		t.Fatal(errFromCP)
	}
	if len(recs.Recs) != 2 {
		t.Fatal("invalid data get from cp")
	}

	recs, errFromCP = conn.DataGetRangeFromCP(222, cp, 1, 10, types.SimpleClass, 2, 8, 0)

	if errFromCP != nil {
		t.Fatal(errFromCP)
	}
	if len(recs.Recs) != 2 {
		t.Fatal("invalid data get from cp")
	}

	recLastValue, errGetLastValue := conn.DataGetLastValue(222, 2, 0)
	if errGetLastValue != nil {
		t.Fatal(errGetLastValue)
	}

	if recLastValue.T != 9 {
		t.Fatal("invalid data get from cp")
	}

	errCloseAll := conn.Close()
	if errCloseAll != nil {
		t.Fatal(errCloseAll)
	}
}

func TestTCPMainBase(t *testing.T) {
	conn := NewConnection("127.0.0.1", "7777", "admin", "admin")
	tm, _ := time.ParseDuration("5s")
	conn.TimeOut = tm
	errConnect := conn.Connect()
	if errConnect != nil {
		t.Fatal(errConnect)
	}

	bl, errGetBaseList := conn.GetBaseList()
	if errGetBaseList == nil {
		for _, val := range *bl {
			if val.Name == "test tcp api" {
				errRemove := conn.RemoveBase("test tcp api")
				if errRemove != nil {
					t.Fatal(errRemove)
				}
				break
			}
		}
	} else {
		t.Fatal(errGetBaseList)
	}

	_, errGetBase := conn.GetBase("test tcp api")
	if errGetBase == nil {
		errRemove := conn.RemoveBase("test tcp api")
		if errRemove != nil {
			t.Fatal(errRemove)
		}
	}

	errAddBase := conn.AddBase(BaseT{
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
	})
	if errAddBase != nil {
		t.Fatal(errAddBase)
	}

	errAddSer := conn.AddSeries("test tcp api", SeriesT{
		Name:        "LINT_single",
		Type:        types.LINT,
		Id:          0,
		Comment:     "",
		ViewTimeMod: 0,
		Looping:     LoopingT{},
		Class:       0,
	})
	if errAddSer != nil {
		t.Fatal(errAddSer)
	}

	errAddSer = conn.AddSeries("test tcp api", SeriesT{
		Name:        "LINT_cache",
		Type:        types.LINT,
		Id:          1,
		Comment:     "",
		ViewTimeMod: 0,
		Looping:     LoopingT{},
		Class:       0,
	})
	if errAddSer != nil {
		t.Fatal(errAddSer)
	}

	errAddSer = conn.AddSeries("test tcp api", SeriesT{
		Name:        "LINT_multi",
		Type:        types.LINT,
		Id:          2,
		Comment:     "",
		ViewTimeMod: 0,
		Looping:     LoopingT{},
		Class:       0,
	})
	if errAddSer != nil {
		t.Fatal(errAddSer)
	}

	sers, errGetSer := conn.GetAllSeries("test tcp api")
	if errGetSer != nil {
		t.Fatal(errGetSer)
	}

	log.Println(sers)

	errOpen2 := conn.OpenBase(2, "test tcp api")
	if errOpen2 != nil {
		t.Fatal(errOpen2)
	}

	errClose := conn.CloseBase(2)
	if errClose != nil {
		t.Fatal(errClose)
	}

	errOpen := conn.OpenBase(222, "test tcp api")
	if errOpen != nil {
		t.Fatal(errOpen)
	}

	for i := 0; i < 10; i++ {
		errAdd := conn.DataAddRow(222, 0, 0, int64(i), 92, i)
		if errAdd != nil {
			t.Fatal(errAdd)
		}
	}

	rowsForCache := conn.NewRows()
	for i := 0; i < 10; i++ {
		errAdd := rowsForCache.DataAddRow(1, 0, int64(i), 92, i)
		if errAdd != nil {
			t.Fatal(errAdd)
		}
	}
	count, errAddCache := conn.DataAddRowCache(222, rowsForCache)
	if errAddCache != nil {
		t.Fatal(errAddCache)
	}
	if count != len(rowsForCache.cache)+8 {
		t.Fatal("incorrect count size")
	}

	rowsForCache = conn.NewRows()
	for i := 0; i < 10; i++ {
		errAdd := rowsForCache.DataAddRow(2, 0, int64(i), 92, i)
		if errAdd != nil {
			t.Fatal(errAdd)
		}
	}
	errAddRows := conn.DataAddRows(222, rowsForCache)
	if errAddRows != nil {
		t.Fatal(errAddRows)
	}
	errClose = conn.CloseBase(222)
	if errClose != nil {
		t.Fatal(errClose)
	}

	errAddBase = conn.UpdateBase("test tcp api", BaseT{
		Name:    "test tcp api rename",
		Comment: "test tcp api rename",
		Path:    "./db/test_tcp_api_rename",
		Looping: LoopingT{
			Type: 0,
			Lt:   "",
		},
		DbSize:        "100m",
		FsType:        FS_FS,
		AutoAddSeries: true,
		AutoSave:      false,
	})
	if errAddBase != nil {
		t.Fatal(errAddBase)
	}
}
