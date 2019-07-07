package record

import (
	"os"
	"encoding/binary"
	"errors"
	"io"
	"hash/crc32"
)

const (
	CHUNK_HEADER_SIZE 	= 7
	BLOCK_SIZE			= 32 * 1024
	CHUNK_FULL			= 1
	CHUNK_FIRST			= 2
	CHUNK_MID			= 3
	CHUNK_LAST			= 4
)

var (
	too_much_data_error = errors.New("不支持的写入长度")
	write_loss_error	= errors.New("输入磁盘出现了错误")
)

type RecordWriter interface {
	io.Reader
	io.Writer
}

type BaseWriter struct {
	cur_path string
	cur_file *os.File
	buf [BLOCK_SIZE]byte
	j int
}

type chunkHeader struct {
	check_sum uint32
	length uint16
	chunk_type byte
}

func (writer *BaseWriter) writeHead(check_sum uint32, length uint16, chunk_type byte) {
	binary.LittleEndian.PutUint32(writer.buf[writer.j:], check_sum)
	binary.LittleEndian.PutUint16(writer.buf[writer.j+4:], uint16(length))
	writer.buf[writer.j+6] = chunk_type
}

func (writer *BaseWriter) write(b []byte) (n int, err error) {
	length := len(b)
	if length > 2 * BLOCK_SIZE - 2 * CHUNK_HEADER_SIZE {
		return 0, too_much_data_error
	}
	check_sum := crc32.ChecksumIEEE(b)
	if writer.j + CHUNK_HEADER_SIZE <= BLOCK_SIZE {
		binary.LittleEndian.PutUint32(writer.buf[writer.j:], check_sum)
		binary.LittleEndian.PutUint16(writer.buf[writer.j+4:], uint16(length))

		// 是否需要分裂
		c := 0
		if writer.j + CHUNK_HEADER_SIZE + length > BLOCK_SIZE {
			writer.buf[writer.j+6] = CHUNK_FIRST
			c = copy(writer.buf[writer.j+7:], b)
			last_c := c

			err = writer.flush()
			if err != nil {
				return c, err
			}
			if length - c > BLOCK_SIZE {
				writer.writeHead(check_sum, uint16(length), CHUNK_MID)
			} else {
				writer.writeHead(check_sum, uint16(length), CHUNK_LAST)
			}
			last_c = copy(writer.buf[writer.j+7:], b[c:])
			c += last_c
			if length - c > 0 {
				err = writer.flush()
				if err != nil {
					return c, err 
				}
				writer.writeHead(check_sum, uint16(length), CHUNK_LAST)
				last_c = copy(writer.buf[writer.j+7:], b[c:])
				c += last_c
			}
			writer.j += CHUNK_HEADER_SIZE + last_c
		} else {
			writer.buf[writer.j+6] = CHUNK_FULL
			copy(writer.buf[writer.j+7:], b)
			writer.j += CHUNK_HEADER_SIZE + length 
		}
		return c, nil
	} else {
		err = writer.flush()
		if err != nil {
			return 0, err 
		}
		return writer.write(b)
	}
}

func (writer *BaseWriter) flush() error {
	n, err := writer.cur_file.Write(writer.buf[:])
	if err != nil {
		return err
	}
	if n != BLOCK_SIZE {
		return write_loss_error
	}
	writer.cur_file.Sync()
	writer.j = 0
	return nil
}