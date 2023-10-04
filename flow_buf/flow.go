package flow_buf

import (
	"sync"
)

const (
	String = 0
	Int    = 1
	Float  = 2
	Bool   = 3
	Byte   = 4
	Map    = 5
	Bytes  = 6
)

type FlowBufT struct {
	buf      []byte // Буфер с данными
	l        int    // Длинна виртуального буфера
	stepSize int    // Размер сегмента буфера
	offset   int    // Указатель виртуального внутри буфера
	pacType  int8   // Тип пакета для кодирования
	empty    bool
}

var poolBytes26 = sync.Pool{
	New: func() interface{} {
		return make([]byte, 26)
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

var poolBytes2 = sync.Pool{
	New: func() interface{} {
		return make([]byte, 2)
	},
}

// NewFlow создание нового патока дл кодирования
// pacType - тип пакета
// size - предположительный или точный размер буфера
// 0 - размер буфера не может быть увеличен передается только pacType
// -1 - размер буфера считается автоматически
func NewFlow(pacType int, size int) FlowBufT {
	if size < 0 {
		size = 1000
	}
	FlowBuf := FlowBufT{
		pacType:  int8(pacType),
		buf:      make([]byte, size+5),
		offset:   5,
		l:        size + 5,
		stepSize: size,
	}
	if size == 0 {
		FlowBuf.empty = true
	}
	return FlowBuf
}

func NewFlowFromBuf(b []byte) FlowBufT {
	FlowBuf := FlowBufT{
		pacType: 0,
		buf:     b,
		offset:  0,
	}
	return FlowBuf
}

func (b *FlowBufT) SetBuf(buf *[]byte) {
	b.buf = *buf
}

func (b *FlowBufT) GetBuf() *[]byte {
	return &b.buf
}

func (b *FlowBufT) GetPack() []byte {
	if !b.empty {
		var offset int
		if b.pacType < 0 {
			offset = 1
		}
		b.buf[0] = byte(b.pacType)
		if b.empty {

		}
		lenPack := poolBytes4.Get().([]byte)
		Int32ToBytesSliceAs(int32(b.offset-5), &lenPack)
		copy(b.buf[1:5], lenPack)
		poolBytes4.Put(lenPack)
		return b.buf[offset:b.offset]
	}
	return []byte{byte(b.pacType)}
}

func (b *FlowBufT) extSize(size int) {
	if size > b.l {
		extSize := b.stepSize
		if size > b.stepSize {
			extSize = size
		}
		b.buf = append(b.buf, make([]byte, extSize)...)
		b.l += extSize
	}
}

func (b *FlowBufT) AddString(val string) {
	l := len(val)
	lenPack := poolBytes4.Get().([]byte)
	Int32ToBytesSliceAs(int32(l), &lenPack)
	newOffset := b.offset + 4
	b.extSize(newOffset + l)
	copy(b.buf[b.offset:newOffset], lenPack)
	b.offset = newOffset
	poolBytes4.Put(lenPack)
	if l == 0 {
		return
	}
	copy(b.buf[b.offset:b.offset+l], val)
	b.offset += l
}

func (b *FlowBufT) GetString() (string, bool) {
	newOffset := b.offset + 4
	if len(b.buf) < newOffset {
		return "", false
	}
	lenString := int(ByteSliceToInt32(b.buf[b.offset:newOffset]))
	b.offset = newOffset
	if len(b.buf) < b.offset+lenString {
		return "", false
	}
	if lenString == 1 && b.buf[b.offset] == 0 {
		b.offset += lenString
		return "", true
	}
	ret := string(b.buf[b.offset : b.offset+lenString])
	b.offset += lenString
	return ret, true
}

func (b *FlowBufT) AddInt(val int) {
	newOffset := b.offset + 8
	b.extSize(newOffset)
	t := poolBytes8.Get().([]byte)
	Int64ToBinSliceAs(int64(val), &t)
	copy(b.buf[b.offset:newOffset], t)
	b.offset = newOffset
	poolBytes8.Put(t)
}

func (b *FlowBufT) GetInt() (int, bool) {
	newOffset := b.offset + 8
	if len(b.buf) < newOffset {
		return 0, false
	}
	ret := int(ByteSliceToInt64(b.buf[b.offset:newOffset]))
	b.offset = newOffset
	return ret, true
}

func (b *FlowBufT) AddInt16(val int16) {
	newOffset := b.offset + 2
	b.extSize(newOffset)
	t := poolBytes2.Get().([]byte)
	Int16ToBytesSliceAs(val, &t)
	copy(b.buf[b.offset:newOffset], t)
	b.offset = newOffset
	poolBytes2.Put(t)
}
func (b *FlowBufT) GetInt16() (int16, bool) {
	newOffset := b.offset + 2
	if len(b.buf) < newOffset {
		return 0, false
	}
	ret := ByteSliceToInt16(b.buf[b.offset:newOffset])
	b.offset = newOffset
	return ret, true
}

func (b *FlowBufT) AddInt32(val int32) {
	newOffset := b.offset + 4
	b.extSize(newOffset)
	t := poolBytes4.Get().([]byte)
	Int32ToBytesSliceAs(val, &t)
	copy(b.buf[b.offset:newOffset], t)
	b.offset = newOffset
	poolBytes4.Put(t)
}

func (b *FlowBufT) GetInt32() (int32, bool) {
	newOffset := b.offset + 4
	if len(b.buf) < newOffset {
		return 0, false
	}
	ret := ByteSliceToInt32(b.buf[b.offset:newOffset])
	b.offset = newOffset
	return ret, true
}

func (b *FlowBufT) AddInt64(val int64) {
	b.AddInt(int(val))
}

func (b *FlowBufT) GetInt64() (int64, bool) {
	newOffset := b.offset + 8
	if len(b.buf) < newOffset {
		return 0, false
	}
	ret := ByteSliceToInt64(b.buf[b.offset:newOffset])
	b.offset = newOffset
	return ret, true
}

func (b *FlowBufT) AddUInt64(val uint64) {
	b.AddInt(int(val))
}

func (b *FlowBufT) GetUInt64() (uint64, bool) {
	newOffset := b.offset + 8
	if len(b.buf) < newOffset {
		return 0, false
	}
	ret := ByteSliceToInt64(b.buf[b.offset:newOffset])
	b.offset = newOffset
	return uint64(ret), true
}

func (b *FlowBufT) AddFloat(val float64) {
	newOffset := b.offset + 8
	b.extSize(newOffset)
	t := poolBytes8.Get().([]byte)
	Float64ToBytesAs(val, &t)
	copy(b.buf[b.offset:newOffset], t)
	b.offset = newOffset
	poolBytes8.Put(t)
}

func (b *FlowBufT) GetFloat() (float64, bool) {
	newOffset := b.offset + 8
	if len(b.buf) < newOffset {
		return 0, false
	}
	ret := ByteSliceToFloat64(b.buf[b.offset:newOffset])
	b.offset = newOffset
	return ret, true
}

func (b *FlowBufT) AddBytes(val []byte) {
	newOffset := b.offset + len(val)
	b.extSize(newOffset)

	copy(b.buf[b.offset:newOffset], val)
	b.offset = newOffset
}

func (b *FlowBufT) GetBytes(length int) ([]byte, bool) {
	newOffset := b.offset + length
	if len(b.buf) < newOffset {
		return nil, false
	}
	ret := make([]byte, length)
	copy(ret, b.buf[b.offset:newOffset])
	b.offset = newOffset
	return ret, true
}

func (b *FlowBufT) AddByte(val byte) {
	newOffset := b.offset + 1
	b.extSize(newOffset)
	b.buf[b.offset] = val
	b.offset = newOffset
}

func (b *FlowBufT) GetByte() (byte, bool) {
	if len(b.buf) < b.offset+1 {
		return 0, false
	}
	ret := b.buf[b.offset]
	b.offset += 1
	return ret, true
}

func (b *FlowBufT) AddBool(val bool) {
	var v byte
	if val {
		v = 1
	}
	newOffset := b.offset + 1
	b.extSize(newOffset)
	b.buf[b.offset] = v
	b.offset = newOffset
}

func (b *FlowBufT) GetBool() (bool, bool) {
	newOffset := b.offset + 1
	if len(b.buf) < newOffset {
		return false, false
	}
	ret := b.buf[b.offset:newOffset]
	b.offset = newOffset
	if ret[0] == 0 {
		return false, true
	}
	return true, true
}

func (b *FlowBufT) AddMap(m map[string]interface{}) {
	count := len(m)
	b.AddInt(count)
	for k, v := range m {
		b.AddString(k)
		switch val := v.(type) {
		case string:
			b.AddInt(String) // string
			b.AddString(val)
		case int:
			b.AddInt(Int) // int
			b.AddInt(val)
		case int64:
			b.AddInt(Int) // int
			b.AddInt(int(val))
		case float64:
			b.AddInt(Float) // float
			b.AddFloat(val)
		case float32:
			b.AddInt(Float) // float
			b.AddFloat(float64(val))
		case bool:
			b.AddInt(Bool) // bool
			b.AddBool(val)
		case byte:
			b.AddInt(Byte) // byte
			b.AddByte(val)
		case map[string]interface{}:
			b.AddInt(Map) // map[string]interface{}
			b.AddMap(val)
		case []byte:
			b.AddInt(Bytes) // []byte
			b.AddInt(len(val))
			b.AddBytes(val)
		}
	}
}

func (b *FlowBufT) GetMap() (map[string]interface{}, bool) {
	count, ok := b.GetInt()
	if !ok {
		return nil, false
	}

	ret := map[string]interface{}{}

	for i := 0; i < count; i++ {
		key, okKey := b.GetString()
		if !okKey {
			return nil, false
		}
		typ, okTyp := b.GetInt()
		if !okTyp {
			return nil, false
		}
		switch typ {
		case String: // string
			v, okV := b.GetString()
			if !okV {
				return nil, false
			}
			ret[key] = v
		case Int:
			v, okV := b.GetInt()
			if !okV {
				return nil, false
			}
			ret[key] = v
		case Float:
			v, okV := b.GetFloat()
			if !okV {
				return nil, false
			}
			ret[key] = v
		case Bool:
			v, okV := b.GetBool()
			if !okV {
				return nil, false
			}
			ret[key] = v
		case Byte:
			v, okV := b.GetByte()
			if !okV {
				return nil, false
			}
			ret[key] = v
		case Map:
			v, okV := b.GetMap()
			if !okV {
				return nil, false
			}
			ret[key] = v
		case Bytes:
			c, okC := b.GetInt()
			if !okC {
				return nil, false
			}
			v, okV := b.GetBytes(c)
			if !okV {
				return nil, false
			}
			ret[key] = v
		}

	}

	return ret, true
}
