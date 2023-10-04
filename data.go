package avads_db_connector

import (
	"encoding/binary"
	"errors"
	"sync"

	"avads_db_connector/flow_buf"
	"avads_db_connector/types"
)

const (
	toMax = byte(1)
	toMin = byte(2)
)

var poolBytes29 = sync.Pool{
	New: func() interface{} {
		return make([]byte, 29)
	},
}

var poolBytes8 = sync.Pool{
	New: func() interface{} {
		return make([]byte, 8)
	},
}

var poolBytes4 = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4)
	},
}

type RowsCacheT struct {
	count int
	cache []byte
}

type RowT struct {
	T     int64
	Value []byte
	Q     []byte
}

type RecWitchCP struct {
	Recs            []RowT
	StartCP         string
	EndCP           string
	HasContinuation bool
}

type BoundaryT struct {
	Min      int64
	Max      int64
	RowCount int
	StartCP  string
	EndCP    string
}

type ErrorInfoT struct {
	Number int
	Name   string
}

func (c *ConnectionT) NewRows() RowsCacheT {
	return RowsCacheT{cache: []byte{}, count: 0}
}

func (r *RowsCacheT) Count() int {
	return r.count
}

func (r *RowsCacheT) Len() int {
	return len(r.cache)
}

func (r *RowsCacheT) DataAddRow(seriesId int64, class byte, t int64, q uint32, value interface{}) error {
	switch class {
	case types.SimpleClass: // атомарный
		tBytes8 := poolBytes8.Get().([]byte)
		tBytes4 := poolBytes4.Get().([]byte)
		body := poolBytes29.Get().([]byte)

		// пакуем seriesId параметра
		binary.BigEndian.PutUint64(tBytes8, uint64(seriesId))
		copy(body[:8], tBytes8)

		// базовый клас параметра
		body[8] = 0

		// метка времени
		binary.BigEndian.PutUint64(tBytes8, uint64(t))
		copy(body[9:17], tBytes8)

		// значение
		err := convVal.ValToBinaryAs(value, tBytes8)
		if err != nil {
			poolBytes8.Put(tBytes8)
			poolBytes4.Put(tBytes4)
			poolBytes29.Put(body)
			return err
		}
		copy(body[17:25], tBytes8)

		binary.BigEndian.PutUint32(tBytes4, q)

		// признак качества
		copy(body[25:29], tBytes4)

		r.cache = append(r.cache, body...)

		poolBytes8.Put(tBytes8)
		poolBytes4.Put(tBytes4)
		poolBytes29.Put(body)
	case types.BlobClass: // BLOB
		tBytes8 := poolBytes8.Get().([]byte)
		tBytes4 := poolBytes4.Get().([]byte)
		t4 := poolBytes4.Get().([]byte)
		body := make([]byte, 21)

		// пакуем seriesId параметра
		binary.BigEndian.PutUint64(tBytes8, uint64(seriesId))
		copy(body, tBytes8)

		// базовый клас параметра
		body[8] = 1

		// метка времени
		binary.BigEndian.PutUint64(tBytes8, uint64(t))
		copy(body[9:17], tBytes8)

		// значение
		val := value.(string)
		binary.BigEndian.PutUint32(t4, uint32(len(val)))
		copy(body[17:21], t4)
		body = append(body, val...)
		// признак качества
		binary.BigEndian.PutUint32(tBytes4, q)

		body = append(body, tBytes4...)

		r.cache = append(r.cache, body...)

		poolBytes8.Put(tBytes8)
		poolBytes4.Put(t4)
	}
	r.count++
	return nil
}

func (c *ConnectionT) DataAddRows(baseId int, r RowsCacheT) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}
	sendFlow := flow_buf.NewFlow(DataAddRows, len(r.cache)+8)
	sendFlow.AddInt(baseId)
	sendFlow.AddBytes(r.cache)
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return errS
	}
	c.cmd.Unlock()
	return nil
}
func (c *ConnectionT) DataMathFunc(baseId int, seriesId int, min int64, max int64, algorithm int) (map[string]interface{}, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}
	sendFlow := flow_buf.NewFlow(DataMathFunc, 40)
	sendFlow.AddInt(baseId)
	sendFlow.AddInt(seriesId)
	sendFlow.AddInt64(min)
	sendFlow.AddInt64(max)
	sendFlow.AddInt(algorithm)
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return nil, errS
	}

	inPck, err := c.stream.readAnswer()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	c.cmd.Unlock()
	readBuf := flow_buf.NewFlowFromBuf(*inPck)
	ret, ok := readBuf.GetMap()
	if !ok {
		return nil, errors.New("invalid result")
	}
	return ret, nil
}

func (c *ConnectionT) DataAddRowCache(baseId int, r RowsCacheT) (int, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return 0, errConnect
	}
	sendFlow := flow_buf.NewFlow(DataAddRowCache, 8+len(r.cache))
	sendFlow.AddInt(baseId)
	sendFlow.AddBytes(r.cache)
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return 0, err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return 0, err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return 0, errS
	}

	inPck, err := c.stream.readAnswer()
	if err != nil {
		c.cmd.Unlock()
		return 0, err
	}
	c.cmd.Unlock()
	readBuf := flow_buf.NewFlowFromBuf(*inPck)
	count, _ := readBuf.GetInt()

	return count, nil
}

func (c *ConnectionT) DataAddRow(baseId int, seriesId int, class byte, t int64, q uint32, val interface{}) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}
	var zVal *[]byte
	addLen := 8
	switch class {
	case types.BlobClass:
		var errToVal error
		zVal, errToVal = convVal.ValToBinary(val)
		if errToVal != nil {
			return errToVal
		}
		addLen = 4 + len(*zVal)
	}
	sendFlow := flow_buf.NewFlow(DataAddRow, 29+addLen)
	sendFlow.AddInt(baseId)
	sendFlow.AddInt(seriesId)
	sendFlow.AddByte(class)
	sendFlow.AddInt(int(t))
	switch class {
	case types.SimpleClass:
		tmpVal := poolBytes8.Get().([]byte)
		copy(tmpVal, []byte{0, 0, 0, 0, 0, 0, 0, 0})

		err := convVal.ValToBinaryAs(val, tmpVal)
		if err != nil {
			return err
		}
		sendFlow.AddBytes(tmpVal)
		poolBytes8.Put(tmpVal)
	case types.BlobClass:
		sendFlow.AddInt32(int32(len(*zVal)))
		sendFlow.AddBytes(*zVal)
	}

	tBytes4 := poolBytes4.Get().([]byte)
	binary.BigEndian.PutUint32(tBytes4, q)
	sendFlow.AddBytes(tBytes4)
	poolBytes4.Put(tBytes4)
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return errS
	}
	c.cmd.Unlock()
	return nil
}

func (c *ConnectionT) DataGetLastValue(baseId int, seriesId int, class int) (*RowT, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}
	sendFlow := flow_buf.NewFlow(DataGetLastValue, 16)
	sendFlow.AddInt(baseId)
	sendFlow.AddInt(seriesId)
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return nil, errS
	}
	i, err := c.stream.GetLenPacket()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	pckAns := make([]byte, i)
	_, err = c.stream.GetBuff(&pckAns, i)
	c.cmd.Unlock()
	if err != nil {
		return nil, err
	}
	readBuf := flow_buf.NewFlowFromBuf(pckAns)
	Time, okT := readBuf.GetInt64() // time
	if !okT {
		return nil, errors.New("invalid rec format")
	}

	var Value []byte
	var okValue bool

	switch class {
	case types.SimpleClass:
		Value, okValue = readBuf.GetBytes(8) // time
		if !okValue {
			return nil, errors.New("invalid rec format")
		}
	case types.BlobClass:
		tmpString, okValueString := readBuf.GetString()
		if !okValueString {
			return nil, errors.New("invalid rec format")
		}
		Value = []byte(tmpString)
	}

	Q, okQ := readBuf.GetBytes(4)
	if !okQ {
		return nil, errors.New("invalid rec format")
	}

	return &RowT{
		T:     Time,
		Value: Value,
		Q:     Q,
	}, nil

}

func (c *ConnectionT) DataGetValueAtTime(baseId int, seriesId int, t int64, class int) (*RowT, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}

	sendFlow := flow_buf.NewFlow(DataGetValueAtTime, 24)
	sendFlow.AddInt(baseId)
	sendFlow.AddInt(seriesId)
	sendFlow.AddInt(int(t))
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return nil, errS
	}
	i, err := c.stream.GetLenPacket()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	pckAns := make([]byte, i)
	_, err = c.stream.GetBuff(&pckAns, i)
	c.cmd.Unlock()
	if err != nil {
		return nil, err
	}
	readBuf := flow_buf.NewFlowFromBuf(pckAns)
	Time, okT := readBuf.GetInt64() // time
	if !okT {
		return nil, errors.New("invalid rec format")
	}

	var Value []byte
	var okValue bool

	if class == types.SimpleClass {
		Value, okValue = readBuf.GetBytes(8) // time
		if !okValue {
			return nil, errors.New("invalid rec format")
		}
	} else {
		tmpString, okValueString := readBuf.GetString()
		if !okValueString {
			return nil, errors.New("invalid rec format")
		}
		Value = []byte(tmpString)
	}

	Q, okQ := readBuf.GetBytes(4)
	if !okQ {
		return nil, errors.New("invalid rec format")
	}

	return &RowT{
		T:     Time,
		Value: Value,
		Q:     Q,
	}, nil
}

func (c *ConnectionT) DataGetCP(baseId int, seriesId int, t int64) (string, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return "", errConnect
	}

	sendFlow := flow_buf.NewFlow(DataGetCP, 24)
	sendFlow.AddInt(baseId)
	sendFlow.AddInt(seriesId)
	sendFlow.AddInt(int(t))
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return "", err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return "", err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return "", errS
	}

	i, err := c.stream.GetLenPacket()
	if err != nil {
		c.cmd.Unlock()
		return "", err
	}
	pckAns := make([]byte, i)
	_, err = c.stream.GetBuff(&pckAns, i)
	c.cmd.Unlock()
	if err != nil {
		return "", err
	}
	readBuf := flow_buf.NewFlowFromBuf(pckAns)
	CP, okStartCP := readBuf.GetString()
	if !okStartCP {
		return "", errors.New("invalid data")
	}

	return CP, nil
}

func (c *ConnectionT) DataGetRangeDirection(baseId int, seriesId int, class int, direct byte, limit int, min int64, max int64, dpi int16) (*RecWitchCP, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}

	sendFlow := flow_buf.NewFlow(DateGetRangeDirection, 43)
	sendFlow.AddInt(baseId)
	sendFlow.AddInt(seriesId)
	sendFlow.AddByte(direct)
	sendFlow.AddInt(limit)
	sendFlow.AddInt(int(min))
	sendFlow.AddInt(int(max))
	sendFlow.AddInt16(dpi)

	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return nil, errS
	}

	i, err := c.stream.GetLenPacket()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	pckAns := make([]byte, i)
	_, err = c.stream.GetBuff(&pckAns, i)
	c.cmd.Unlock()
	if err != nil {
		return nil, err
	}
	readBuf := flow_buf.NewFlowFromBuf(pckAns)
	rec := RecWitchCP{}
	StartCP, okStartCP := readBuf.GetString()
	if !okStartCP {
		return nil, errors.New("invalid data")
	}
	EndCP, okEndCP := readBuf.GetString()
	if !okEndCP {
		return nil, errors.New("invalid data")
	}
	HasContinuation, okHasContinuation := readBuf.GetBool()
	if !okHasContinuation {
		return nil, errors.New("invalid data")
	}
	recs, okRecs := readBuf.GetInt()
	if !okRecs {
		return nil, errors.New("invalid data")
	}
	rec.Recs = make([]RowT, recs)
	rec.EndCP = EndCP
	rec.StartCP = StartCP
	rec.HasContinuation = HasContinuation
	for ind := 0; ind < recs; ind++ {
		T, okT := readBuf.GetInt64()
		if !okT {
			return nil, errors.New("invalid data")
		}

		var Value []byte
		var okValue bool

		switch class {
		case types.SimpleClass:
			Value, okValue = readBuf.GetBytes(8) // time
			if !okValue {
				return nil, errors.New("invalid rec format")
			}
		case types.BlobClass:
			tmpString, okValueString := readBuf.GetString()
			if !okValueString {
				return nil, errors.New("invalid rec format")
			}
			Value = []byte(tmpString)
		}

		Q, okQ := readBuf.GetBytes(4)
		if !okQ {
			return nil, errors.New("invalid data")
		}

		rec.Recs[ind] = RowT{
			T:     T,
			Value: Value,
			Q:     Q,
		}
	}
	return &rec, nil
}

func (c *ConnectionT) DataGetFromCP(baseId int, cp string, direct byte, limit int, class int) (*RecWitchCP, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}

	sendFlow := flow_buf.NewFlow(DataGetFromCP, len(cp)+21)
	sendFlow.AddInt(baseId)
	sendFlow.AddString(cp)
	sendFlow.AddByte(direct)
	sendFlow.AddInt(limit)
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return nil, errS
	}

	i, err := c.stream.GetLenPacket()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	pckAns := make([]byte, i)
	_, err = c.stream.GetBuff(&pckAns, i)
	c.cmd.Unlock()
	if err != nil {
		return nil, err
	}
	readBuf := flow_buf.NewFlowFromBuf(pckAns)
	rec := RecWitchCP{}
	StartCP, okStartCP := readBuf.GetString()
	if !okStartCP {
		return nil, errors.New("invalid data")
	}
	EndCP, okEndCP := readBuf.GetString()
	if !okEndCP {
		return nil, errors.New("invalid data")
	}
	HasContinuation, okHasContinuation := readBuf.GetBool()
	if !okHasContinuation {
		return nil, errors.New("invalid data")
	}
	recs, okRecs := readBuf.GetInt()
	if !okRecs {
		return nil, errors.New("invalid data")
	}
	rec.Recs = make([]RowT, recs)
	rec.EndCP = EndCP
	rec.StartCP = StartCP
	rec.HasContinuation = HasContinuation
	for ind := 0; ind < recs; ind++ {
		T, okT := readBuf.GetInt64()
		if !okT {
			return nil, errors.New("invalid data")
		}

		var Value []byte
		var okValue bool

		switch class {
		case types.SimpleClass:
			Value, okValue = readBuf.GetBytes(8) // time
			if !okValue {
				return nil, errors.New("invalid rec format")
			}
		case types.BlobClass:
			tmpString, okValueString := readBuf.GetString()
			if !okValueString {
				return nil, errors.New("invalid rec format")
			}
			Value = []byte(tmpString)
		}

		Q, okQ := readBuf.GetBytes(4)
		if !okQ {
			return nil, errors.New("invalid data")
		}

		rec.Recs[ind] = RowT{
			T:     T,
			Value: Value,
			Q:     Q,
		}
	}
	return &rec, nil
}

func (c *ConnectionT) DataGetRangeFromCP(baseId int, cp string, direct byte, limit int, class int, min int64, max int64, dpi int16) (*RecWitchCP, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}

	sendFlow := flow_buf.NewFlow(DateGetRangeFromCP, len(cp)+39)
	sendFlow.AddInt(baseId)
	sendFlow.AddString(cp)
	sendFlow.AddByte(direct)
	sendFlow.AddInt(limit)
	sendFlow.AddInt(int(min))
	sendFlow.AddInt(int(max))
	sendFlow.AddInt16(dpi)
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return nil, errS
	}

	i, err := c.stream.GetLenPacket()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	pckAns := make([]byte, i)
	_, err = c.stream.GetBuff(&pckAns, i)
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	c.cmd.Unlock()
	readBuf := flow_buf.NewFlowFromBuf(pckAns)
	rec := RecWitchCP{}
	StartCP, okStartCP := readBuf.GetString()
	if !okStartCP {
		return nil, errors.New("invalid data")
	}
	EndCP, okEndCP := readBuf.GetString()
	if !okEndCP {
		return nil, errors.New("invalid data")
	}
	HasContinuation, okHasContinuation := readBuf.GetBool()
	if !okHasContinuation {
		return nil, errors.New("invalid data")
	}
	recs, okRecs := readBuf.GetInt()
	if !okRecs {
		return nil, errors.New("invalid data")
	}
	rec.Recs = make([]RowT, recs)
	rec.EndCP = EndCP
	rec.StartCP = StartCP
	rec.HasContinuation = HasContinuation
	for ind := 0; ind < recs; ind++ {
		T, okT := readBuf.GetInt64()
		if !okT {
			return nil, errors.New("invalid data")
		}

		var Value []byte
		var okValue bool

		switch class {
		case types.SimpleClass:
			Value, okValue = readBuf.GetBytes(8) // time
			if !okValue {
				return nil, errors.New("invalid rec format")
			}
		case types.BlobClass:
			tmpString, okValueString := readBuf.GetString()
			if !okValueString {
				return nil, errors.New("invalid rec format")
			}
			Value = []byte(tmpString)
		}

		Q, okQ := readBuf.GetBytes(4)
		if !okQ {
			return nil, errors.New("invalid data")
		}

		rec.Recs[ind] = RowT{
			T:     T,
			Value: Value,
			Q:     Q,
		}
	}
	return &rec, nil
}

func (c *ConnectionT) DataDeleteRow(baseId int, seriesId int, t int64) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}

	sendFlow := flow_buf.NewFlow(DataDeleteRow, 24)
	sendFlow.AddInt(baseId)
	sendFlow.AddInt(seriesId)
	sendFlow.AddInt(int(t))
	pck := sendFlow.GetPack()

	_, err := c.stream.Write(pck)
	if err != nil {
		return err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		return err
	}
	if status != 0 {
		return c.stream.getError()
	}
	return nil
}

func (c *ConnectionT) DataDeleteRows(baseId int, seriesId int, TimeStart int64, TimeEnd int64) (int, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return 0, errConnect
	}

	sendFlow := flow_buf.NewFlow(DataDeleteRows, 32)
	sendFlow.AddInt(baseId)
	sendFlow.AddInt(seriesId)
	sendFlow.AddInt(int(TimeStart))
	sendFlow.AddInt(int(TimeEnd))
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return 0, err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return 0, err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return 0, errS
	}
	i, err := c.stream.GetLenPacket()
	if err != nil {
		c.cmd.Unlock()
		return 0, err
	}
	pckAns := make([]byte, i)
	_, err = c.stream.GetBuff(&pckAns, i)
	c.cmd.Unlock()
	if err != nil {
		return 0, err
	}
	readBuf := flow_buf.NewFlowFromBuf(pckAns)
	count, okCount := readBuf.GetInt()
	if !okCount {
		return 0, errors.New("invalid data")
	}
	return count, nil
}

func (c *ConnectionT) DataGetBoundary(baseId int, seriesId int) (*BoundaryT, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}

	sendFlow := flow_buf.NewFlow(DataGetBoundary, 16)
	sendFlow.AddInt(baseId)
	sendFlow.AddInt(seriesId)
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}

	status, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	if status != 0 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return nil, errS
	}

	i, err := c.stream.GetLenPacket()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	pckAns := make([]byte, i)
	_, err = c.stream.GetBuff(&pckAns, i)
	c.cmd.Unlock()
	if err != nil {
		return nil, err
	}
	readBuf := flow_buf.NewFlowFromBuf(pckAns)
	min, okMin := readBuf.GetInt64()
	if !okMin {
		return nil, errors.New("invalid data")
	}
	max, okMax := readBuf.GetInt64()
	if !okMax {
		return nil, errors.New("invalid data")
	}
	rowCount, okRowCount := readBuf.GetInt()
	if !okRowCount {
		return nil, errors.New("invalid data")
	}

	startCP, okStartCP := readBuf.GetString()
	if !okStartCP {
		return nil, errors.New("invalid data")
	}
	endCP, okEndCP := readBuf.GetString()
	if !okEndCP {
		return nil, errors.New("invalid data")
	}

	return &BoundaryT{
		Min:      min,
		Max:      max,
		RowCount: rowCount,
		StartCP:  startCP,
		EndCP:    endCP,
	}, nil
}

func (c *ConnectionT) GetAddRowCacheErrors() ([]ErrorInfoT, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}

	sendFlow := flow_buf.NewFlow(GetAddRowCacheErrors, 0)
	pck := sendFlow.GetPack()
	c.cmd.Lock()
	_, err := c.stream.Write(pck)
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}

	state, err := c.stream.readAnswerCode()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	if state == 1 {
		errS := c.stream.getError()
		c.cmd.Unlock()
		return nil, errS
	}

	lenPack, err := c.stream.GetLenPacket()
	if err != nil {
		c.cmd.Unlock()
		return nil, err
	}
	pckAns := make([]byte, lenPack)
	_, err = c.stream.GetBuff(&pckAns, lenPack)
	c.cmd.Unlock()
	if err != nil {
		return nil, err
	}
	readBuf := flow_buf.NewFlowFromBuf(pckAns)
	count, _ := readBuf.GetInt()
	var ret = make([]ErrorInfoT, count)

	for i := 0; i < count; i++ {
		Number, okNumber := readBuf.GetInt()
		if !okNumber {
			return nil, errors.New("broken packet")
		}
		Name, okString := readBuf.GetString()
		if !okString {
			return nil, errors.New("broken packet")
		}
		ret[i] = ErrorInfoT{
			Number: Number,
			Name:   Name,
		}
	}

	return ret, nil
}
