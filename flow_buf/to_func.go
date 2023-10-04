package flow_buf

import "math"

func ByteSliceToInt16(b []byte) int16 {
	return int16(b[1]) | int16(b[0])<<8
}

func Int16ToBytesSliceAs(val int16, b *[]byte) {
	p := *b
	p[0] = byte(val >> 8)
	p[1] = byte(val)
}

func ByteSliceToInt32(b []byte) int32 {
	return int32(b[3]) | int32(b[2])<<8 | int32(b[1])<<16 | int32(b[0])<<24
}

func Int32ToBytesSliceAs(val int32, b *[]byte) {
	p := *b
	p[0] = byte(val >> 24)
	p[1] = byte(val >> 16)
	p[2] = byte(val >> 8)
	p[3] = byte(val)
}

func Int64ToBinSliceAs(v int64, b *[]byte) {
	val := uint64(v)
	p := *b
	p[0] = byte(val >> 56)
	p[1] = byte(val >> 48)
	p[2] = byte(val >> 40)
	p[3] = byte(val >> 32)
	p[4] = byte(val >> 24)
	p[5] = byte(val >> 16)
	p[6] = byte(val >> 8)
	p[7] = byte(val)
}

func ByteSliceToInt64(b []byte) int64 {
	nb := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	copy(nb[8-len(b):], b)
	return int64(nb[7]) | int64(nb[6])<<8 | int64(nb[5])<<16 | int64(nb[4])<<24 |
		int64(nb[3])<<32 | int64(nb[2])<<40 | int64(nb[1])<<48 | int64(nb[0])<<56
}

func Float64ToBytesAs(val float64, b *[]byte) {
	t := math.Float64bits(val)
	p := *b
	p[0] = byte(t >> 56)
	p[1] = byte(t >> 48)
	p[2] = byte(t >> 40)
	p[3] = byte(t >> 32)
	p[4] = byte(t >> 24)
	p[5] = byte(t >> 16)
	p[6] = byte(t >> 8)
	p[7] = byte(t)
}
func ByteSliceToFloat64(b []byte) float64 {
	nb := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	copy(nb[8-len(b):], b)
	var t uint64
	t = uint64(nb[7]) | uint64(nb[6])<<8 | uint64(nb[5])<<16 | uint64(nb[4])<<24 |
		uint64(nb[3])<<32 | uint64(nb[2])<<40 | uint64(nb[1])<<48 | uint64(nb[0])<<56
	return math.Float64frombits(t)
}
