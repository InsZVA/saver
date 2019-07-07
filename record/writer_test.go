package record

import (
	"testing"
	"os"
	"io/ioutil"
	"bytes"
	"hash/crc32"
	"encoding/binary"
	"reflect"
	"math/rand"
	"time"
)

func newTestWriter(t *testing.T) *BaseWriter {
	f, err := os.Create("/tmp/record")
	if err != nil {
		t.Error(err)
	}

	writer := &BaseWriter {
		cur_file: f,
	}
	return writer
}

func randomSlice(max_length int) []byte {
	length := rand.Intn(max_length)
	sed := make([]byte, 0, 32)
	for i := 0; i < length && i < 32; i++ {
		sed = append(sed, byte('a' + byte(i)))
	}
	return repeatSlice(sed, length / 32)
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
	writer := newTestWriter(t)
	writer.writeHead(uint32(0x8f71d34f), uint16(23), CHUNK_FIRST)
	err := writer.flush()
	if err != nil {
		t.Error(err)
	}
	writer.cur_file.Close()

	data, err := ioutil.ReadFile("/tmp/record")
	if err != nil {
		t.Error(err)
	}
	data = data[:CHUNK_HEADER_SIZE]
	exp := []byte("\x4f\xd3\x71\x8f\x17\x00\x02")
	expect(t, exp, data)
}

var chunk_types = []string{"", "full", "first", "mid", "last"}

func checkData(data []byte, d []byte, i int, t *testing.T) int {
	check_sum := crc32.ChecksumIEEE(data)
	left := BLOCK_SIZE - i % BLOCK_SIZE
	next_block := i + left
	// 下一个块中处理
	if left < CHUNK_HEADER_SIZE {
		i += left
		return checkData(data, d, i, t)
	}
	t.Log("check:", i)

	length := int(binary.LittleEndian.Uint16(d[i+4:]))
	t.Log("length:", length)
	raw_length := len(data)
	first := true
	for len(data) != 0 {
		expect(t, binary.LittleEndian.Uint32(d[i:]), check_sum)
		expect(t, length, raw_length)
		chunk_type := d[i+6]
		t.Logf("Find chunk: %s", chunk_types[chunk_type])

		if chunk_type == CHUNK_FULL {
			if first {
				if i + len(data) + CHUNK_HEADER_SIZE > next_block {
					t.Errorf("长度超过了当前BLOCK，但是被标记为CHUNK_FULL")
				}
				expect(t, data, d[i+CHUNK_HEADER_SIZE:i+CHUNK_HEADER_SIZE+length])
				i += CHUNK_HEADER_SIZE + len(data)
				break
			} else {
				t.Errorf("分割chunk中出现了First")
			}
		} else if chunk_type == CHUNK_FIRST {
			if !first {
				t.Errorf("没有LAST，却出现了First")
			}
			if i + len(data) + CHUNK_HEADER_SIZE <= next_block {
				t.Errorf("长度不需要分割，但是被标记为CHUNK_FIRST")
			}
			expect(t, data[:left-CHUNK_HEADER_SIZE], d[i+CHUNK_HEADER_SIZE:next_block])
			i = next_block
			next_block += BLOCK_SIZE
			data = data[left-CHUNK_HEADER_SIZE:]
			first = false
			continue
		} else if chunk_type == CHUNK_MID {
			if first {
				t.Errorf("没有FIRST的情况下出现了MID")
			}
			if i + len(data) + CHUNK_HEADER_SIZE <= next_block {
				t.Errorf("剩余的长度只需要一个CHUNK_LAST即可，但是出现了CHUNK_MID")
			}
			expect(t, data[:BLOCK_SIZE-CHUNK_HEADER_SIZE], d[i+CHUNK_HEADER_SIZE:next_block])
			i = next_block
			next_block += BLOCK_SIZE
			data = data[BLOCK_SIZE:]
			continue
		} else if chunk_type == CHUNK_LAST {
			if first {
				t.Errorf("没有FIRST的情况下出现了LAST")
			}
			if i + len(data) + CHUNK_HEADER_SIZE > next_block {
				t.Errorf("剩余长度不足以在LAST写完")
			}
			expect(t, data[:], d[i+CHUNK_HEADER_SIZE:i+CHUNK_HEADER_SIZE+len(data)])
			i += CHUNK_HEADER_SIZE + len(data)
			break
		}
		t.Errorf("错误chunk %d, idx: %d", chunk_type, i)
		// TODO
		break
	}
	return i
}

func Test_BaseWriter_write(t *testing.T) {
	writer := newTestWriter(t)
	// 简单测试
	data1 := repeatSlice([]byte("abc"), 1)
	writer.write(data1)
	// 测试不跨块（生成FULL）
	data2 := repeatSlice([]byte("a"), BLOCK_SIZE - 3 - CHUNK_HEADER_SIZE * 2 - 1)
	writer.write(data2)
	// 测试两块
	data3 := repeatSlice([]byte("b"), BLOCK_SIZE * 2 - CHUNK_HEADER_SIZE * 2 - CHUNK_HEADER_SIZE)
	writer.write(data3)
	// 测试空间只剩header
	data4 := repeatSlice([]byte("c"), BLOCK_SIZE - CHUNK_HEADER_SIZE)
	writer.write(data4)
	// 普通测试，留不到一个header的空间
	data5 := repeatSlice([]byte("a"), BLOCK_SIZE - CHUNK_HEADER_SIZE * 2 + 3)
	writer.write(data5)
	// 测试header空间不够
	data6 := repeatSlice([]byte("xascvasdf"), 1)
	writer.write(data6)
	// 测试数据太大
	data7 := repeatSlice([]byte("x"), BLOCK_SIZE * 2 + 1)
	n, err := writer.write(data7)
	expect(t, n, 0)
	expect(t, err, too_much_data_error)
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
	for length < size * 1024 * 1024 {
		data := randomSlice(256)
		datas = append(datas, data)
		length += len(data)
	}

	start := time.Now().UnixNano()
	writer := newTestWriter(nil)
	for _, data := range datas {
		writer.write(data)
	}
	b.Logf("写%dM随机数据，每条大概128字节，共计耗时%dms", size, (time.Now().UnixNano() - start) / 1e9)
}