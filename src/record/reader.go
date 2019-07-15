package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

type BaseReader struct {
	// 底层的reader
	reader io.Reader
	// 内存buffer
	buf [blockSize]byte
	// Header
	header chunkHeader
	// buf的长度，除了文件的最后一行，其余时候都等于blockSize
	n int
	// 当前读取指针所在的位置
	j int
	// 当前数据的开始位置
	s int
	// 是否开始过数据读取
	start bool
	// 读取过程中存在的错误
	err error
}

func (reader *BaseReader) nextBlock() error {
	n, err := reader.reader.Read(reader.buf[:])
	reader.n = n
	reader.err = err
	return err
}

func (reader *BaseReader) Recover() error {
	// TODO
	return nil
}

func (reader *BaseReader) NextChunk() error {
	if reader.j+chunkHeaderSize > blockSize {
		// 跳到下一个Block继续读
		err := reader.nextBlock()
		if err != nil && err != io.EOF {
			return err
		}
		return reader.NextChunk()
	}
	checkSum := uint32(binary.LittleEndian.Uint32(reader.buf[reader.j:]))
	length := int(binary.LittleEndian.Uint16(reader.buf[reader.j+4:]))

	chunkType := reader.buf[reader.j+6]
	// TODO: blockSize -> n
	if reader.j+length+chunkHeaderSize > blockSize {
		reader.err = errors.New("长度超过了当前BLOCK，但是被标记为chunkFull")
		return reader.Recover()
	}
	reader.header.checkSum = checkSum
	reader.header.chunkType = chunkType
	reader.header.length = uint16(length)

	reader.s = reader.j + chunkHeaderSize
	reader.j = reader.s + length
	return nil
}

func (reader *BaseReader) ReadRecord() ([]byte, error) {
	cur_data := make([]byte, 0)
	err := reader.NextChunk()
	if err != nil {
		return nil, err
	}
	cur_data = append(cur_data, reader.buf[reader.s:reader.j]...)
	if reader.header.chunkType == chunkFull {
	} else if reader.header.chunkType == chunkFirst {
		if len(cur_data) >= int(reader.header.length) {
			return nil, errors.New("chunk不需要分裂，但是出现了chunkFirst")
		}
		err = reader.NextChunk()
		if err != nil {
			return nil, err
		}
		cur_data = append(cur_data, reader.buf[reader.s:reader.j]...)
		if reader.header.chunkType == chunkMid {
			if len(cur_data) >= int(reader.header.length) {
				return nil, errors.New("chunk不需要分裂，但是出现了chunkMid")
			}
			err = reader.NextChunk()
			if err != nil {
				return nil, err
			}
			cur_data = append(cur_data, reader.buf[reader.s:reader.j]...)
			if reader.header.chunkType != chunkLast {
				return nil, errors.New("chunkMid之后出现了非Last的chunk")
			}
		} else if reader.header.chunkType == chunkLast {
		} else {
			return nil, errors.New("chunkFirst之后出现了非Mid非Last的chunk")
		}
	} else {
		return nil, errors.New("一个chunk的开始必须是chunkFirst或者chunkFull")
	}

	if len(cur_data) != int(reader.header.length) {
		return nil, errors.New("chunkFull没有含有全部的数据")
	}
	if reader.header.checkSum != crc32.ChecksumIEEE(cur_data) {
		return nil, errors.New("校验和错误")
	}
	return cur_data, nil
}
