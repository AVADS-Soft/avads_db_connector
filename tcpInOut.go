package avads_db_connector

import (
	"avads_db_connector/flow_buf"
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

type LteDBInOutTCP struct {
	tcpInOut  *net.TCPConn
	reader    *bufio.Reader
	writer    *bufio.Writer
	timeout   time.Duration
	version   int64
	IsConnect bool
}

func (l *LteDBInOutTCP) SetTimeout(tm time.Duration) {
	l.timeout = tm
}

func (l *LteDBInOutTCP) Close() error {
	if l.tcpInOut != nil {
		err := l.tcpInOut.Close()
		return err
	}
	return nil
}

func (l *LteDBInOutTCP) Read(p []byte) (int, error) {
	if l.tcpInOut == nil {
		return 0, errors.New("connection not found")
	}
	if l.timeout > 0 {
		l.tcpInOut.SetReadDeadline(time.Now().Add(l.timeout))
	}
	return l.tcpInOut.Read(p)
}

func (l *LteDBInOutTCP) Write(p []byte) (int, error) {
	if l.tcpInOut == nil {
		return 0, errors.New("connection not found")
	}
	if l.timeout > 0 {
		l.tcpInOut.SetWriteDeadline(time.Now().Add(l.timeout))
	}
	n, err := l.tcpInOut.Write(p)
	if err != nil {
		l.IsConnect = false
	}
	return n, err
}

func (l *LteDBInOutTCP) Init(Address string, Port string) error {
	servAddr := Address + ":" + Port
	tcpAddr, err := net.ResolveTCPAddr("tcp", servAddr)
	if err != nil {
		return err
	}
	d := net.Dialer{Timeout: l.timeout}
	conn, err := d.Dial("tcp", tcpAddr.String())

	if err != nil {
		return err
	}
	l.tcpInOut, _ = conn.(*net.TCPConn)

	l.reader = bufio.NewReader(l.tcpInOut)
	l.writer = bufio.NewWriter(l.tcpInOut)

	// запрос версии протокола
	// GetProtocolVersion

	_, errGetProto := l.Write([]byte{GetProtocolVersion})
	if errGetProto != nil {
		return errGetProto
	}

	state, errAnswer := l.readAnswerCode()
	if errAnswer != nil {
		return errAnswer
	}
	if state == 1 {
		return l.getError()
	}

	lenPacket, err := l.GetLenPacket()
	if err != nil {
		return err
	}
	msg := make([]byte, lenPacket)
	_, err = l.GetBuff(&msg, lenPacket)
	if err != nil {
		return err
	}
	l.version = int64(msg[0])
	return nil
}

// readAnswerCode чтение первого байта информации
func (l *LteDBInOutTCP) readAnswerCode() (byte, error) {
	if l.timeout > 0 {
		l.tcpInOut.SetReadDeadline(time.Now().Add(l.timeout))
	}
	b, errRead := l.reader.ReadByte()
	if errRead != nil {
		if errors.Is(errRead, io.EOF) {
			l.IsConnect = false
			return 0, errRead
		}
		if os.IsTimeout(errRead) {
			return 0, errRead
		}
	}
	return b, nil
}

// readAnswer чтение пакета
func (l *LteDBInOutTCP) readAnswer() (*[]byte, error) {
	lenString, errLen := l.GetLenPacket()
	if errLen != nil {
		if errors.Is(errLen, io.EOF) {
			l.IsConnect = false
			return nil, errLen
		}
	}
	msg := make([]byte, lenString)
	_, errStr := l.GetBuff(&msg, lenString)
	if errStr != nil {
		return nil, errStr
	}
	return &msg, nil
}

// readAnswerCode чтение первого байта информации
func (l *LteDBInOutTCP) getError() error {
	i, errLen := l.GetLenPacket()
	if errLen != nil {
		if errLen == io.EOF {
			l.IsConnect = false
			return errLen
		}
	}

	pckAns := make([]byte, i)
	_, err := l.GetBuff(&pckAns, i)
	if err != nil {
		return err
	}
	readBuf := flow_buf.NewFlowFromBuf(pckAns)

	msg, _ := readBuf.GetString()
	return errors.New(msg)
}

// GetLenPacket получение длинны пакета из стрима данных
func (l *LteDBInOutTCP) GetLenPacket() (int32, error) {
	lp := []byte{0, 0, 0, 0}
	size := 0
	for {
		if l.timeout > 0 {
			l.tcpInOut.SetReadDeadline(time.Now().Add(l.timeout))
		}
		i, err := l.reader.Read(lp[size:])
		if err != nil {
			return 0, errors.New("error read len packet " + err.Error())
		}
		size += i
		if size == 4 {
			return flow_buf.ByteSliceToInt32(lp), nil
		}
	}
}

// GetBuff получения буфера из стрима указанной длинны
func (l *LteDBInOutTCP) GetBuff(pck *[]byte, lenPack int32) (int, error) {
	var err error
	mSize := lenPack
	if mSize > 1024 {
		mSize = 1024
	}
	buffer := make([]byte, mSize)
	rLen := 0
	i := 0
	for {
		if int32(rLen)+mSize > lenPack {
			mSize = lenPack - int32(rLen)
		}
		if l.timeout > 0 {
			l.tcpInOut.SetReadDeadline(time.Now().Add(l.timeout))
		}
		i, err = l.reader.Read(buffer[:mSize])
		if err != nil {
			if errors.Is(err, io.EOF) {
				l.IsConnect = false
				break
			}
			err = errors.New(fmt.Sprintf("Read error - %s\n", err))
			break
		}
		copy((*pck)[rLen:], buffer[:i])
		rLen += i
		if rLen == int(lenPack) {
			break
		}
	}
	return rLen, err
}

func (l *LteDBInOutTCP) WritePacket(buff *[]byte) (int, error) {
	size := len(*buff)
	if l.writer.Available() >= len(*buff) {
		if l.timeout > 0 {
			l.tcpInOut.SetWriteDeadline(time.Now().Add(l.timeout))
		}
		n, err := l.writer.Write(*buff)
		if err != nil {
			return n, err
		}
		err = l.writer.Flush()
		return n, err
	}
	o := 0
	nRet := 0
	for {
		s := l.writer.Available()
		if size-o < 1 {
			return nRet, nil
		}
		if size < o+s {
			s = size - o
		}
		if l.timeout > 0 {
			l.tcpInOut.SetWriteDeadline(time.Now().Add(l.timeout))
		}
		sendBuf := (*buff)[o : o+s]
		n, err := l.writer.Write(sendBuf)
		if err != nil {
			return n, err
		}
		err = l.writer.Flush()
		nRet += n
		if err != nil {
			return nRet, err
		}
		o += s
	}
}
