package avads_db_connector

import "avads_db_connector/flow_buf"

type SeriesT struct {
	Name        string   `json:"name"`              // Программное имя ряда мах len (255)
	Type        int      `json:"type"`              // Тип параметра
	Id          int64    `json:"id"`                // Id ряда
	Comment     string   `json:"comment"`           // Комментарий к ряду max len (255)
	ViewTimeMod int      `json:"view_time_mod"`     // Тип отображения времени
	Looping     LoopingT `json:"looping,omitempty"` // Настройки зацикливания
	Class       byte     `json:"class"`
}

// GetAllSeries получение списка временных рядов
func (c *ConnectionT) GetAllSeries(baseName string) ([]*SeriesT, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}
	sendFlow := flow_buf.NewFlow(SeriesGetAll, len(baseName)+4)
	sendFlow.AddString(baseName)
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
	count, _ := readBuf.GetInt()

	retArray := make([]*SeriesT, count)

	for i := 0; i < count; i++ {
		ret := SeriesT{Looping: LoopingT{}}

		ret.Id, _ = readBuf.GetInt64()
		ret.Name, _ = readBuf.GetString()
		ret.Class, _ = readBuf.GetByte()
		ret.Type, _ = readBuf.GetInt()
		tViewMod, _ := readBuf.GetByte()
		ret.ViewTimeMod = int(tViewMod)
		ret.Comment, _ = readBuf.GetString()
		ret.Looping.Type, _ = readBuf.GetByte()
		ret.Looping.Lt, _ = readBuf.GetString()
		retArray[i] = &ret
	}
	return retArray, nil
}

// GetSeries получение информации о временном ряде
func (c *ConnectionT) GetSeries(baseName string, name string) (*SeriesT, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}
	sendFlow := flow_buf.NewFlow(SeriesGetInfo, len(baseName)+len(name)+8)
	sendFlow.AddString(baseName)
	sendFlow.AddString(name)
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
	ret := SeriesT{Looping: LoopingT{}}

	ret.Id, _ = readBuf.GetInt64()
	ret.Name, _ = readBuf.GetString()
	ret.Class, _ = readBuf.GetByte()
	ret.Type, _ = readBuf.GetInt()
	tViewMod, _ := readBuf.GetByte()
	ret.ViewTimeMod = int(tViewMod)
	ret.Comment, _ = readBuf.GetString()
	ret.Looping.Type, _ = readBuf.GetByte()
	ret.Looping.Lt, _ = readBuf.GetString()

	return &ret, nil
}

// GetSeriesById получение информации о временном ряде
func (c *ConnectionT) GetSeriesById(baseName string, seriesId int) (*SeriesT, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}
	sendFlow := flow_buf.NewFlow(SeriesGetInfoById, len(baseName)+12)
	sendFlow.AddString(baseName)
	sendFlow.AddInt64(int64(seriesId))
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
	ret := SeriesT{Looping: LoopingT{}}

	ret.Id, _ = readBuf.GetInt64()
	ret.Name, _ = readBuf.GetString()
	ret.Class, _ = readBuf.GetByte()
	ret.Type, _ = readBuf.GetInt()
	tViewMod, _ := readBuf.GetByte()
	ret.ViewTimeMod = int(tViewMod)
	ret.Comment, _ = readBuf.GetString()
	ret.Looping.Type, _ = readBuf.GetByte()
	ret.Looping.Lt, _ = readBuf.GetString()

	return &ret, nil
}

// RemoveSeries удаление временного ряда
func (c *ConnectionT) RemoveSeries(baseName string, seriesId int) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}
	sendFlow := flow_buf.NewFlow(SeriesRemove, len(baseName)+12)
	sendFlow.AddString(baseName)
	sendFlow.AddInt(seriesId)
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

// AddSeries добавление нового временного ряда
func (c *ConnectionT) AddSeries(baseName string, series SeriesT) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}
	l := len(baseName) + len(series.Name) + len(series.Comment) + len(series.Looping.Lt) + 34
	sendFlow := flow_buf.NewFlow(SeriesCreate, l)
	sendFlow.AddString(baseName)
	sendFlow.AddInt(int(series.Id))
	sendFlow.AddString(series.Name)
	sendFlow.AddInt(series.Type)
	sendFlow.AddByte(byte(series.ViewTimeMod))
	sendFlow.AddString(series.Comment)
	sendFlow.AddByte(series.Looping.Type)
	sendFlow.AddString(series.Looping.Lt)

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

// UpdateSeries обновление свойств временного ряда
func (c *ConnectionT) UpdateSeries(baseName string, series SeriesT) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}
	l := len(baseName) + len(series.Name) + len(series.Comment) + len(series.Looping.Lt) + 34
	sendFlow := flow_buf.NewFlow(SeriesUpdate, l)
	sendFlow.AddString(baseName)
	sendFlow.AddInt(int(series.Id))
	sendFlow.AddString(series.Name)
	sendFlow.AddInt(series.Type)
	sendFlow.AddByte(byte(series.ViewTimeMod))
	sendFlow.AddString(series.Comment)
	sendFlow.AddByte(series.Looping.Type)
	sendFlow.AddString(series.Looping.Lt)

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
