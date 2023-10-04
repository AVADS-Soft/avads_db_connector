package flow_buf

import (
	"avads_db_connector/types"
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"
)

type ConvertT struct {
	Order binary.ByteOrder
}

func NewConvert(order binary.ByteOrder) *ConvertT {
	return &ConvertT{Order: order}
}

func (c *ConvertT) SetOrder(order binary.ByteOrder) {
	c.Order = order
}

// ValToBinary перевод переменной неопределенного типа в массив байт
func (c *ConvertT) ValToBinary(v interface{}) (*[]byte, error) {
	var err error

	switch vl := v.(type) {
	case []byte:
		return &vl, nil
	case [8]byte:
		ret := vl[:]
		return &ret, nil
	case complex64, complex128:
		buf := new(bytes.Buffer)
		err = binary.Write(buf, c.Order, v)
		ret := buf.Bytes()
		return &ret, err
	case float32:
		return c.Float32ToBinSlice(vl), nil
	case float64:
		return c.Float64ToBinSlice(vl), nil
	case int64:
		return c.Int64ToBinSlice(vl), nil
	case int:
		return c.Int64ToBinSlice(int64(vl)), nil
	case int32:
		return c.Int32ToBinSlice(vl), nil
	case int16:
		return c.Int16ToBinSlice(vl), nil
	case int8:
		return c.Int8ToBinSlice(vl), nil
	case uint64:
		return c.UInt64ToBinSlice(vl), nil
	case uint:
		return c.UInt64ToBinSlice(uint64(vl)), nil
	case uint32:
		return c.UInt32ToBinSlice(vl), nil
	case uint16:
		return c.UInt16ToBinSlice(vl), nil
	case uint8:
		return c.UInt8ToBinSlice(vl), nil
	case bool:
		return c.Bool8ToBinSlice(vl), nil
	case string:
		ret := []byte(vl)
		return &ret, nil
	default:
		return nil, errors.New("invalid type")
	}
}

func (c *ConvertT) ValToBinaryAs(v interface{}, bArray []byte) error {
	switch vl := v.(type) {
	case bool:
		c.BoolToBytesSliceAs(vl, &bArray)
	case []byte:
		copy(bArray, vl)
	case string:
		copy(bArray, vl)
	case int64:
		c.Int64ToBinSliceAs(vl, &bArray)
	case int:
		c.Int64ToBinSliceAs(int64(vl), &bArray)
	case int32:
		c.Int32ToBytesSliceAs(vl, &bArray)
	case int16:
		c.Int16ToBytesSliceAs(vl, &bArray)
	case int8:
		c.Int8ToBytesSliceAs(vl, &bArray)
	case uint64:
		c.UInt64ToBinSliceAs(vl, &bArray)
	case uint:
		c.UInt64ToBinSliceAs(uint64(vl), &bArray)
	case uint32:
		c.UInt32ToBytesSliceAs(vl, &bArray)
	case uint16:
		c.UInt16ToBytesSliceAs(vl, &bArray)
	case uint8:
		c.UInt8ToBytesSliceAs(vl, &bArray)
	case float64:
		c.Float64ToBytesAs(vl, &bArray)
	case float32:
		c.Float32ToBytesAs(vl, &bArray)
	default:
		panic("unknown convert")
	}
	return nil
}

func (c *ConvertT) UInt64ToBinSlice(val uint64) *[]byte {
	p := make([]byte, 8)
	c.Order.PutUint64(p, val)
	return &p
}

func (c *ConvertT) UInt64ToBinSliceAs(val uint64, b *[]byte) {
	c.Order.PutUint64(*b, val)
}

func (c *ConvertT) UInt32ToBinSlice(val uint32) *[]byte {
	p := make([]byte, 8)
	c.Order.PutUint64(p, uint64(val))
	return &p
}

func (c *ConvertT) UInt32ToBytesSliceAs(val uint32, b *[]byte) {
	c.Order.PutUint64(*b, uint64(val))
}

func (c *ConvertT) UInt16ToBinSlice(val uint16) *[]byte {
	p := make([]byte, 8)
	c.Order.PutUint64(p, uint64(val))
	return &p
}

func (c *ConvertT) UInt16ToBytesSliceAs(val uint16, b *[]byte) {
	c.Order.PutUint64(*b, uint64(val))
}

func (c *ConvertT) UInt8ToBinSlice(val uint8) *[]byte {
	p := make([]byte, 8)
	c.Order.PutUint64(p, uint64(val))
	return &p
}

func (c *ConvertT) BoolToBytesSliceAs(val bool, b *[]byte) {
	var v uint64
	if val {
		v = 1
	}
	c.Order.PutUint64(*b, v)
}

func (c *ConvertT) Bool8ToBinSlice(val bool) *[]byte {
	var v uint64
	if val {
		v = 1
	}
	p := make([]byte, 8)
	c.Order.PutUint64(p, v)
	return &p
}

func (c *ConvertT) UInt8ToBytesSliceAs(val uint8, b *[]byte) {
	c.Order.PutUint64(*b, uint64(val))
}

func (c *ConvertT) Int64ToBinSlice(v int64) *[]byte {
	p := make([]byte, 8)
	c.Order.PutUint64(p, uint64(v))
	return &p
}

func (c *ConvertT) Int64ToBinSliceAs(v int64, b *[]byte) {
	c.Order.PutUint64(*b, uint64(v))
}

func (c *ConvertT) Int32ToBinSlice(val int32) *[]byte {
	p := make([]byte, 8)
	c.Order.PutUint64(p, uint64(val))
	return &p
}

func (c *ConvertT) Int32ToBytesSliceAs(val int32, b *[]byte) {
	c.Order.PutUint64(*b, uint64(val))
}

func (c *ConvertT) Int16ToBinSlice(val int16) *[]byte {
	p := make([]byte, 8)
	c.Order.PutUint64(p, uint64(val))
	return &p
}

func (c *ConvertT) Int16ToBytesSliceAs(val int16, b *[]byte) {
	c.Order.PutUint64(*b, uint64(val))
}

func (c *ConvertT) Int8ToBinSlice(val int8) *[]byte {
	p := make([]byte, 8)
	c.Order.PutUint64(p, uint64(val))
	return &p
}

func (c *ConvertT) Int8ToBytesSliceAs(val int8, b *[]byte) {
	c.Order.PutUint64(*b, uint64(val))
}

func (c *ConvertT) Float64ToBinSlice(val float64) *[]byte {
	t := math.Float64bits(val)
	p := make([]byte, 8)
	c.Order.PutUint64(p, t)
	return &p
}

func (c *ConvertT) Float64ToBytesAs(val float64, b *[]byte) {
	t := math.Float64bits(val)
	c.Order.PutUint64(*b, t)
}

func (c *ConvertT) Float32ToBinSlice(val float32) *[]byte {
	t := math.Float64bits(float64(val))
	p := make([]byte, 8)
	c.Order.PutUint64(p, t)
	return &p
}

func (c *ConvertT) Float32ToBytesAs(val float32, b *[]byte) {
	t := math.Float64bits(float64(val))
	c.Order.PutUint64(*b, t)
}

func (c *ConvertT) ByteToChar(b []byte) interface{} {
	return string(b[:1])
}

func (c *ConvertT) ByteSliceToBool(b []byte) bool {
	tVal := c.Order.Uint64(b)
	ret := true
	if tVal == 0 {
		ret = false
	}
	return ret
}

func (c *ConvertT) ByteToBool(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	ret := true
	if tVal == 0 {
		ret = false
	}
	return ret
}

func (c *ConvertT) ByteToByte(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return uint8(tVal)

}

func (c *ConvertT) ByteToInt8(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return int8(tVal)
}

func (c *ConvertT) ByteSliceToInt8(b []byte) int8 {
	tVal := c.Order.Uint64(b)
	return int8(tVal)
}

func (c *ConvertT) ByteToUInt8(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return uint8(tVal)
}
func (c *ConvertT) ByteSliceToUInt8(b []byte) uint8 {
	tVal := c.Order.Uint64(b)
	return uint8(tVal)
}

func (c *ConvertT) ByteToInt16(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return int16(tVal)
}

func (c *ConvertT) ByteSliceToInt16(b []byte) int16 {
	tVal := c.Order.Uint64(b)
	return int16(tVal)
}

func (c *ConvertT) ByteToUInt16(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return uint16(tVal)
}
func (c *ConvertT) ByteSliceToUInt16(b []byte) uint16 {
	tVal := c.Order.Uint64(b)
	return uint16(tVal)
}

func (c *ConvertT) ByteToInt32(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return int32(tVal)
}
func (c *ConvertT) ByteSliceToInt32(b []byte) int32 {
	tVal := c.Order.Uint64(b)
	return int32(tVal)
}

func (c *ConvertT) ByteToUInt32(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return uint32(tVal)
}

func (c *ConvertT) ByteSliceToUInt32(b []byte) uint32 {
	tVal := c.Order.Uint64(b)
	return uint32(tVal)
}

func (c *ConvertT) ByteToInt64(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return int64(tVal)
}
func (c *ConvertT) ByteSliceToInt64(b []byte) int64 {
	tVal := c.Order.Uint64(b)
	return int64(tVal)
}

func (c *ConvertT) ByteToUInt64(b []byte) interface{} {
	return c.Order.Uint64(b)
}
func (c *ConvertT) ByteSliceToUInt64(b []byte) uint64 {
	return c.Order.Uint64(b)
}

func (c *ConvertT) ByteToFloat64(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return math.Float64frombits(tVal)
}
func (c *ConvertT) ByteSliceToFloat64(b []byte) float64 {
	tVal := c.Order.Uint64(b)
	return math.Float64frombits(tVal)
}

func (c *ConvertT) ByteToFloat32(b []byte) interface{} {
	tVal := c.Order.Uint64(b)
	return float32(math.Float64frombits(tVal))
}
func (c *ConvertT) ByteSliceToFloat32(b []byte) float32 {
	tVal := c.Order.Uint64(b)
	return float32(math.Float64frombits(tVal))
}

func (c *ConvertT) BoolToBytes(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func (c *ConvertT) ByteToString(b []byte) string {
	s := string(b)
	return strings.Split(s, string([]byte{0}))[0]
}

// GetConverter получение функции конвертации от типа
func (c *ConvertT) GetConverter(typ int) func([]byte) interface{} {
	switch typ {
	case types.LINT:
		return c.ByteToInt64
	case types.DINT:
		return c.ByteToInt32
	case types.INT:
		return c.ByteToInt16
	case types.SINT:
		return c.ByteToInt8
	case types.ULINT:
		return c.ByteToUInt64
	case types.UDINT:
		return c.ByteToUInt32
	case types.UINT:
		return c.ByteToUInt16
	case types.USINT:
		return c.ByteToUInt8
	case types.REAL:
		return c.ByteToFloat32
	case types.LREAL:
		return c.ByteToFloat64
	case types.BOOL:
		return c.ByteToBool
	case types.BYTE:
		return c.ByteToByte
	case types.CHAR:
		return c.ByteToChar
	case types.STRING:
		return BytesToString
	case types.WSTRING:
		return BytesToWString
	default:
		return nil
	}
}

func BytesToString(val []byte) interface{} {
	return string(val)
}

func BytesToWString(val []byte) interface{} {
	return string(val)
}
