package avads_db_connector

import (
	"time"

	"avads_db_connector/flow_buf"
)

const (
	FS_FS        = "fs"    // Файл базы пишется единым сегментом
	FS_MULTIPART = "fs_mp" // Файл бары разбивается по кускам 1 гб
	FS_MEMORY    = "mem_fs"
)

type BaseT struct {
	Name             string   `json:"name"`               // Программное имя базы (должно быть уникальным)
	Comment          string   `json:"comment"`            // Комментарий к базе
	Path             string   `json:"path"`               // Физическое место расположение базы
	DataSize         int      `json:"data_size"`          // размер блока данных
	Status           int      `json:"status"`             // Статус базы
	Looping          LoopingT `json:"looping,omitempty"`  // Настройки зацикливания
	DbSize           string   `json:"db_size"`            // Строковое значение предельного размера базы "500gb"
	FsType           string   `json:"fs_type"`            // Тип файловой системы
	AutoAddSeries    bool     `json:"auto_add_series"`    // Автоматическое добавление рядов
	AutoSave         bool     `json:"auto_save"`          // Автоматически сохранять долго неиспользуемый кеш
	AutoSaveDuration string   `json:"auto_save_duration"` // Дельта времени для проверки устаревших данных
	AutoSaveInterval string   `json:"auto_save_interval"` // Время интервала для проверки неиспользованных данных
}

type LoopingT struct {
	Type     byte          `json:"type"` // Type тип зацикливания
	Lt       string        `json:"lt"`   // Lt время жизни данных в формате 72h3m (max len 50)
	LifeTime time.Duration `json:"-"`
}

const (
	LoopInherit  = 0
	LoopNone     = 1
	LoopDuration = 2
)

var OpenBaseList = map[int]string{}
var TryBaseList = map[int]string{}

// GetBaseList получение списка баз
func (c *ConnectionT) GetBaseList() (*[]BaseT, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}
	sendFlow := flow_buf.NewFlow(BaseGetList, 0)
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
	var ret []BaseT

	for i := 0; i < count; i++ {
		item := BaseT{Looping: LoopingT{}}

		item.Name, _ = readBuf.GetString()    // name
		item.Path, _ = readBuf.GetString()    // path
		item.Comment, _ = readBuf.GetString() // comment
		item.Status, _ = readBuf.GetInt()     // status
		item.Looping.Type, _ = readBuf.GetByte()
		item.Looping.Lt, _ = readBuf.GetString()
		item.DbSize, _ = readBuf.GetString()
		item.FsType, _ = readBuf.GetString()
		item.AutoAddSeries, _ = readBuf.GetBool()
		item.AutoSave, _ = readBuf.GetBool()
		item.AutoSaveDuration, _ = readBuf.GetString()
		item.AutoSaveInterval, _ = readBuf.GetString()

		ret = append(ret, item)
	}

	return &ret, nil
}

// GetBase получение информации о базе
func (c *ConnectionT) GetBase(name string) (*BaseT, error) {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return nil, errConnect
	}
	sendFlow := flow_buf.NewFlow(BaseGetInfo, len(name)+4)
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

	ret := BaseT{Looping: LoopingT{}}

	ret.Name, _ = readBuf.GetString()    // name
	ret.Path, _ = readBuf.GetString()    // path
	ret.Comment, _ = readBuf.GetString() // comment
	ret.Status, _ = readBuf.GetInt()     // status
	ret.Looping.Type, _ = readBuf.GetByte()
	ret.Looping.Lt, _ = readBuf.GetString()
	ret.DbSize, _ = readBuf.GetString()
	ret.FsType, _ = readBuf.GetString()
	ret.AutoAddSeries, _ = readBuf.GetBool()
	ret.AutoSave, _ = readBuf.GetBool()
	ret.AutoSaveDuration, _ = readBuf.GetString()
	ret.AutoSaveInterval, _ = readBuf.GetString()
	return &ret, nil
}

// RemoveBase удаление базы
func (c *ConnectionT) RemoveBase(name string) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}
	sendFlow := flow_buf.NewFlow(BaseRemove, len(name)+4)
	sendFlow.AddString(name)
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

// UpdateBase обновление свойств базы
func (c *ConnectionT) UpdateBase(baseName string, base BaseT) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}
	l := len(baseName) + len(base.Name) + len(base.Comment) + len(base.Path) + len(base.DbSize) + len(base.Looping.Lt) + len(base.AutoSaveDuration) + len(base.AutoSaveInterval) + 35
	sendFlow := flow_buf.NewFlow(BaseUpdate, l)
	sendFlow.AddString(baseName)
	sendFlow.AddString(base.Name)
	sendFlow.AddString(base.Comment)
	sendFlow.AddString(base.Path)
	sendFlow.AddString(base.DbSize)
	sendFlow.AddByte(base.Looping.Type)
	sendFlow.AddString(base.Looping.Lt)
	sendFlow.AddBool(base.AutoAddSeries)
	sendFlow.AddBool(base.AutoSave)
	sendFlow.AddString(base.AutoSaveDuration)
	sendFlow.AddString(base.AutoSaveInterval)
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
	for i, v := range OpenBaseList {
		if baseName == v {
			OpenBaseList[i] = base.Name
		}
	}

	return nil
}

// AddBase добавление новой базы
func (c *ConnectionT) AddBase(base BaseT) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}
	l := len(base.Name) + len(base.Comment) + len(base.Path) + len(base.DbSize) + len(base.Looping.Lt) + len(base.AutoSaveDuration) + len(base.AutoSaveInterval) + 31
	sendFlow := flow_buf.NewFlow(BaseCreate, l)
	sendFlow.AddString(base.Name)
	sendFlow.AddString(base.Comment)
	sendFlow.AddString(base.Path)
	sendFlow.AddString(base.FsType)
	sendFlow.AddString(base.DbSize)
	sendFlow.AddByte(base.Looping.Type)
	sendFlow.AddString(base.Looping.Lt)
	sendFlow.AddBool(base.AutoAddSeries)
	sendFlow.AddBool(base.AutoSave)
	sendFlow.AddString(base.AutoSaveDuration)
	sendFlow.AddString(base.AutoSaveInterval)
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
		return errS
	}
	c.cmd.Unlock()
	return nil
}

// OpenBase подключение к базе
func (c *ConnectionT) OpenBase(id int, name string) error {
	TryBaseList[id] = name
	if c.Reconnect.State == 0 {
		errConnect := c.checkConnect()
		if errConnect != nil {
			return errConnect
		}
	}
	sendFlow := flow_buf.NewFlow(BaseOpen, len(name)+12)
	sendFlow.AddInt(id)
	sendFlow.AddString(name)
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
	delete(TryBaseList, id)
	OpenBaseList[id] = name
	return nil
}

// CloseBase закрытие базы
func (c *ConnectionT) CloseBase(id int) error {
	errConnect := c.checkConnect()
	if errConnect != nil {
		return errConnect
	}
	sendFlow := flow_buf.NewFlow(BaseClose, 8)
	sendFlow.AddInt(id)
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
	delete(OpenBaseList, id)
	return nil
}
