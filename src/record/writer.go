package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
)

const (
	chunkHeaderSize = 7
	blockSize       = 32 * 1024
	chunkFull       = 1
	chunkFirst      = 2
	chunkMid        = 3
	chunkLast       = 4
)

var (
	errTooMuchData = errors.New("不支持的写入长度")
	errWriteLoss   = errors.New("输入磁盘出现了错误")
)

// BaseWriter 最基础的记录写入工具
type BaseWriter struct {
	curPath string
	curFile *os.File
	buf     [blockSize]byte
	j       int
}

type chunkHeader struct {
	checkSum  uint32
	length    uint16
	chunkType byte
}

func (writer *BaseWriter) writeHead(checkSum uint32, length uint16, chunkType byte) {
	binary.LittleEndian.PutUint32(writer.buf[writer.j:], checkSum)
	binary.LittleEndian.PutUint16(writer.buf[writer.j+4:], uint16(length))
	writer.buf[writer.j+6] = chunkType
}

func (writer *BaseWriter) write(b []byte) (n int, err error) {
	length := len(b)
	if length > 2*blockSize-2*chunkHeaderSize {
		return 0, errTooMuchData
	}
	checkSum := crc32.ChecksumIEEE(b)
	var c int
	if writer.j+chunkHeaderSize <= blockSize {
		binary.LittleEndian.PutUint32(writer.buf[writer.j:], checkSum)
		binary.LittleEndian.PutUint16(writer.buf[writer.j+4:], uint16(length))

		// 是否需要分裂
		if writer.j+chunkHeaderSize+length > blockSize {
			writer.buf[writer.j+6] = chunkFirst
			c = copy(writer.buf[writer.j+7:], b)
			lastC := c

			err = writer.flush()
			if err != nil {
				return c, err
			}
			if length-c > blockSize {
				writer.writeHead(checkSum, uint16(length), chunkMid)
			} else {
				writer.writeHead(checkSum, uint16(length), chunkLast)
			}
			lastC = copy(writer.buf[writer.j+7:], b[c:])
			c += lastC
			if length-c > 0 {
				err = writer.flush()
				if err != nil {
					return c, err
				}
				writer.writeHead(checkSum, uint16(length), chunkLast)
				lastC = copy(writer.buf[writer.j+7:], b[c:])
				c += lastC
			}
			writer.j += chunkHeaderSize + lastC
		} else {
			writer.buf[writer.j+6] = chunkFull
			copy(writer.buf[writer.j+7:], b)
			writer.j += chunkHeaderSize + length
		}
		// return c, nil
	} else {
		err = writer.flush()
		if err != nil {
			return 0, err
		}
		return writer.write(b)
	}
	return c, nil
}

func (writer *BaseWriter) flush() error {
	n, err := writer.curFile.Write(writer.buf[:])
	if err != nil {
		return err
	}
	if n != blockSize {
		return errWriteLoss
	}
	writer.curFile.Sync()
	writer.j = 0
	return nil
}
