package avads_db_connector

import (
	"avads_db_connector/flow_buf"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Disconnect            = 0
	BaseCreate            = 1
	BaseOpen              = 2
	BaseGetInfo           = 3
	BaseGetList           = 4
	BaseRemove            = 5
	BaseUpdate            = 6
	BaseClose             = 7
	SeriesCreate          = 8
	SeriesRemove          = 9
	SeriesUpdate          = 10
	SeriesGetAll          = 11
	SeriesGetInfo         = 12
	SeriesGetInfoById     = 34
	UserGetList           = 13
	UserGetInfo           = 14
	UserCreate            = 15
	UserRemove            = 16
	UserUpdate            = 17
	PropsGetList          = 18
	PropsGetInfo          = 19
	PropsSet              = 20
	DataGetBoundary       = 21
	DataGetCP             = 22
	DataGetFromCP         = 23
	DateGetRangeFromCP    = 24
	DateGetRangeDirection = 25
	DataAddRow            = 26
	DataDeleteRow         = 27
	DataDeleteRows        = 28
	DataAddRowCache       = 29
	DataGetValueAtTime    = 30
	DataMathFunc          = 31
	DataAddRows           = 32
	DataGetLastValue      = 33
	GetAddRowCacheErrors  = 35
)

const (
	LoginGetKeys       = 0
	LoginValidPass     = 1
	RestoreSession     = 2
	GetProtocolVersion = 254
)

// Преобразователь значений общего назначения
var convVal = flow_buf.ConvertT{Order: binary.BigEndian}

type ConnectionT struct {
	sync.Mutex
	cmd        sync.Mutex // Защита команд от многопоточности
	Address    string
	Port       string
	Login      string
	Pass       string
	SessionKey string
	TimeOut    time.Duration

	Reconnect ReconnectT
	stream    LteDBInOutTCP
}

type ReconnectT struct {
	sync.Mutex
	ReConnectTime time.Duration
	TimeStamp     int64
	State         int64
}

func (c *ConnectionT) Connect() error {
	c.Lock()
	defer c.Unlock()
	c.stream.SetTimeout(c.TimeOut)
	errInit := c.stream.Init(c.Address, c.Port)

	if errInit != nil {
		c.stream.Close()
		return errInit
	}
	size := len(c.Login)
	pck := make([]byte, size+5)
	pck[0] = LoginGetKeys
	lenPack := poolBytes4.Get().([]byte)
	flow_buf.Int32ToBytesSliceAs(int32(size), &lenPack)
	copy(pck[1:], lenPack)
	poolBytes4.Put(lenPack)
	copy(pck[5:], c.Login)
	i, err := c.stream.Write(pck)
	if err != nil {
		c.stream.Close()
		return err
	}
	if i != len(pck) {
		c.stream.Close()
		return errors.New("error send packet")
	}

	code, errReadCode := c.stream.readAnswerCode()
	if errReadCode != nil {
		c.stream.Close()
		return errReadCode
	}
	if code != 0 {
		errMsg := c.stream.getError()
		c.stream.Close()
		return errMsg

	}

	lenPacket, err := c.stream.GetLenPacket()
	if err != nil {
		c.stream.Close()
		return err
	}
	msg := make([]byte, lenPacket)
	_, err = c.stream.GetBuff(&msg, lenPacket)
	if err != nil {
		c.stream.Close()
		return err
	}
	ident := strings.Split(string(msg), string(byte(0)))
	passPSalt := fmt.Sprintf("%x", md5.Sum([]byte(c.Pass+ident[0])))
	passPKay := fmt.Sprintf("%x", md5.Sum([]byte(passPSalt+ident[1])))
	hash := passPKay
	size = len(hash)
	pck = make([]byte, size+5)
	pck[0] = LoginValidPass
	lenPack = poolBytes4.Get().([]byte)
	flow_buf.Int32ToBytesSliceAs(int32(size), &lenPack)
	copy(pck[1:], lenPack)
	copy(pck[5:], hash)
	_, err = c.stream.Write(pck)
	if err != nil {
		return err
	}
	code, errGetCode := c.stream.readAnswerCode()
	if errGetCode != nil {
		errMsg := c.stream.getError()
		c.stream.Close()
		return errMsg
	}
	if code != 0 {
		errMsg := c.stream.getError()
		c.stream.Close()
		return errMsg
	}

	inPck, err := c.stream.readAnswer()
	if err != nil {
		return err
	}
	readBuf := flow_buf.NewFlowFromBuf(*inPck)
	c.SessionKey, _ = readBuf.GetString()

	c.stream.IsConnect = true
	return nil
}

// checkConnect проверка состояния соединения и переподключение если надо
func (c *ConnectionT) checkConnect() error {
	if c.Reconnect.State != 0 {
		return errors.New("reconnecting")
	}
	if c.Reconnect.ReConnectTime > 0 {
		if c.IsClose() {
			if c.Reconnect.TimeStamp-time.Now().Unix() <= 0 {
				err := c.ReConnect()
				if err != nil {
					// переустанавливаем время следующего соединения
					c.Reconnect.TimeStamp = time.Now().Add(c.Reconnect.ReConnectTime).Unix()
					return errors.New("connection lost")
				}
				// коннект восстановлен
				return nil
			}
		}
	}
	return nil
}

func (c *ConnectionT) ReConnect() error {
	if c.SessionKey != "" {
		c.Lock()
		c.stream.SetTimeout(c.TimeOut)
		errInit := c.stream.Init(c.Address, c.Port)

		if errInit != nil {
			c.stream.Close()
			c.Unlock()
			return errInit
		}
		size := len(c.SessionKey)
		pck := make([]byte, size+5)
		pck[0] = RestoreSession

		lenPack := poolBytes4.Get().([]byte)
		flow_buf.Int32ToBytesSliceAs(int32(size), &lenPack)
		poolBytes4.Put(lenPack)

		copy(pck[1:], lenPack)
		copy(pck[5:], c.SessionKey)
		_, err := c.stream.Write(pck)
		if err != nil {
			c.stream.Close()
			c.Unlock()
			return errInit
		}
		code, errGetCode := c.stream.readAnswerCode()
		if errGetCode != nil {
			c.stream.Close()
			c.Unlock()
			return errGetCode
		}
		if code == 0 {
			atomic.StoreInt64(&c.Reconnect.State, 0)
			c.stream.IsConnect = true
			c.Unlock()
			return nil
		}
		c.SessionKey = ""
		c.Unlock()
	}
	c.stream.Close()
	errConnect := c.Connect()
	if errConnect != nil {
		return errConnect
	}
	atomic.StoreInt64(&c.Reconnect.State, 1)
	//go func() {
	for id, name := range OpenBaseList {
		c.OpenBase(id, name)
	}
	for id, name := range TryBaseList {
		c.OpenBase(id, name)
	}
	atomic.StoreInt64(&c.Reconnect.State, 0)
	//}()
	return nil
}

func (c *ConnectionT) IsClose() bool {
	return !c.stream.IsConnect
}

// Close закрытие соединения
func (c *ConnectionT) Close() error {
	c.Lock()
	defer c.Unlock()
	c.stream.IsConnect = false
	return c.stream.Close()
}
