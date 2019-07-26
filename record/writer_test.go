package record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

func newTestWriter(t *testing.T, subfix string) *BaseWriter {
	f, err := os.Create(fmt.Sprintf("/tmp/record_" + subfix))
	if err != nil {
		t.Error(err)
	}

	writer := &BaseWriter{
		curFile: f,
	}
	return writer
}

func randomSlice(maxLength int) []byte {
	ret := make([]byte, 0, maxLength)
	for i := 0; i < maxLength; i++ {
		ret = append(ret, 'a'+byte(rand.Intn(26)))
	}
	return ret
}

func repeatSlice(sed []byte, times int) []byte {
	return bytes.Repeat(sed, times)
}

func expect(t *testing.T, expect interface{}, real interface{}) {
	if !reflect.DeepEqual(expect, real) {
		t.Errorf("Expect: %v, Real: %v", expect, real)
	}
}

func Test_BaseWriter_writeHead(t *testing.T) {
	writer := newTestWriter(t, "writeHead")
	writer.writeHead(uint32(0x8f71d34f), uint16(23), chunkFirst)
	err := writer.flush()
	if err != nil {
		t.Error(err)
	}
	writer.curFile.Close()

	data, err := ioutil.ReadFile("/tmp/record_writeHead")
	if err != nil {
		t.Error(err)
	}
	data = data[:chunkHeaderSize]
	exp := []byte("\x4f\xd3\x71\x8f\x17\x00\x02")
	expect(t, exp, data)
}

var chunkTypes = []string{"", "full", "first", "mid", "last"}

func checkData(data []byte, d []byte, i int, t *testing.T) int {
	checkSum := crc32.ChecksumIEEE(data)
	left := blockSize - i%blockSize
	nextBlock := i + left
	// 下一个块中处理
	if left < chunkHeaderSize {
		i += left
		return checkData(data, d, i, t)
	}
	t.Log("check:", i)

	length := int(binary.LittleEndian.Uint16(d[i+4:]))
	t.Log("length:", length)
	rawLength := len(data)
	first := true
	for len(data) != 0 {
		expect(t, binary.LittleEndian.Uint32(d[i:]), checkSum)
		expect(t, length, rawLength)
		chunkType := d[i+6]
		t.Logf("Find chunk: %s", chunkTypes[chunkType])

		if chunkType == chunkFull {
			if first {
				if i+len(data)+chunkHeaderSize > nextBlock {
					t.Errorf("长度超过了当前BLOCK，但是被标记为chunkFull")
				}
				expect(t, data, d[i+chunkHeaderSize:i+chunkHeaderSize+length])
				i += chunkHeaderSize + len(data)
				break
			} else {
				t.Errorf("分割chunk中出现了First")
			}
		} else if chunkType == chunkFirst {
			if !first {
				t.Errorf("没有LAST，却出现了First")
			}
			if i+len(data)+chunkHeaderSize <= nextBlock {
				t.Errorf("长度不需要分割，但是被标记为chunkFirst")
			}
			expect(t, data[:left-chunkHeaderSize], d[i+chunkHeaderSize:nextBlock])
			i = nextBlock
			nextBlock += blockSize
			data = data[left-chunkHeaderSize:]
			first = false
			continue
		} else if chunkType == chunkMid {
			if first {
				t.Errorf("没有FIRST的情况下出现了MID")
			}
			if i+len(data)+chunkHeaderSize <= nextBlock {
				t.Errorf("剩余的长度只需要一个chunkLast即可，但是出现了chunkMid")
			}
			expect(t, data[:blockSize-chunkHeaderSize], d[i+chunkHeaderSize:nextBlock])
			i = nextBlock
			nextBlock += blockSize
			data = data[blockSize:]
			continue
		} else if chunkType == chunkLast {
			if first {
				t.Errorf("没有FIRST的情况下出现了LAST")
			}
			if i+len(data)+chunkHeaderSize > nextBlock {
				t.Errorf("剩余长度不足以在LAST写完")
			}
			expect(t, data[:], d[i+chunkHeaderSize:i+chunkHeaderSize+len(data)])
			i += chunkHeaderSize + len(data)
			break
		}
		t.Errorf("错误chunk %d, idx: %d", chunkType, i)
		// TODO
		break
	}
	return i
}

func Test_BaseWriter_write(t *testing.T) {
	writer := newTestWriter(t, "write")
	// 简单测试
	data1 := repeatSlice([]byte("abc"), 1)
	writer.write(data1)
	// 测试不跨块（生成FULL）
	data2 := repeatSlice([]byte("a"), blockSize-3-chunkHeaderSize*2-1)
	writer.write(data2)
	// 测试两块
	data3 := repeatSlice([]byte("b"), blockSize*2-chunkHeaderSize*2-chunkHeaderSize)
	writer.write(data3)
	// 测试空间只剩header
	data4 := repeatSlice([]byte("c"), blockSize-chunkHeaderSize)
	writer.write(data4)
	// 普通测试，留不到一个header的空间
	data5 := repeatSlice([]byte("a"), blockSize-chunkHeaderSize*2+3)
	writer.write(data5)
	// 测试header空间不够
	data6 := repeatSlice([]byte("xascvasdf"), 1)
	writer.write(data6)
	// 测试数据太大
	data7 := repeatSlice([]byte("x"), blockSize*2+1)
	n, err := writer.write(data7)
	expect(t, n, 0)
	expect(t, err, errTooMuchData)
	// 测试空数据
	data8 := []byte{}
	n, err = writer.write(data8)
	expect(t, n, 0)
	expect(t, err, nil)

	err = writer.flush()
	if err != nil {
		t.Error(err)
	}

	idx := 0
	d, err := ioutil.ReadFile("/tmp/record")
	if err != nil {
		t.Error(err)
	}
	t.Log(idx)
	idx = checkData(data1, d, idx, t)
	t.Log(idx)
	idx = checkData(data2, d, idx, t)
	t.Log(idx)
	idx = checkData(data3, d, idx, t)
	t.Log(idx)
	idx = checkData(data4, d, idx, t)
	t.Log(idx)
	idx = checkData(data5, d, idx, t)
	t.Log(idx)
	idx = checkData(data6, d, idx, t)
	t.Log(idx)
}

func BenchmarkFile(b *testing.B) {
	size := 4 * 1024
	datas := [][]byte{}
	length := 0
	for length < size*1024*1024 {
		data := randomSlice(256)
		datas = append(datas, data)
		length += len(data)
	}

	start := time.Now().UnixNano()
	writer := newTestWriter(nil, "benchmark")
	for _, data := range datas {
		writer.write(data)
	}
	b.Logf("写%dM随机数据，每条大概128字节，共计耗时%dms", size, (time.Now().UnixNano()-start)/1e9)
}
