package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"table"
)

type SeqFile interface {
	io.Closer
	io.ReadWriter
	io.ReaderAt
	io.WriterAt
	io.Seeker
	Size() int64
}

type BaseFile struct {
	*os.File
	os.FileInfo
}

const (
	bufSize = 32 * 1024
)

type SSTable struct {
	file SeqFile
	buff [bufSize]byte
}

func CreateSSTable(filepath string) (*SSTable, error) {
	file, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		return nil, err
	}
	bf := BaseFile{}
	bf.File = file
	bf.FileInfo = fileInfo
	return &SSTable{
		file: bf,
	}, nil
}

func (sst *SSTable) Close() error {
	return sst.file.Close()
}

/*
[ValLength32, valValue...] 8字节对齐Data
[ValLength32, valValue...]
...
[keyLength32, keyValue...] 8字节对齐Metadata
[keyLength32, keyValue...]
...
*/

type MetaData map[string]uint64

func (md MetaData) AddKey(key []byte, idx uint64) {
	md[string(key)] = idx
}

func (md MetaData) WriteTo(writer io.Writer) (int, error) {
	buff := make([]byte, 0, len(md)*64)
	keyLength := make([]byte, 4)
	idxBuff := make([]byte, 8)
	for key, idx := range md {
		buff = buff[:0]
		binary.LittleEndian.PutUint32(keyLength, uint32(len(key)))
		buff = append(buff, keyLength...)
		buff = append(buff, key...)
		binary.LittleEndian.PutUint64(idxBuff, uint64(idx))
		if len(buff)%8 != 0 {
			for i := 0; i < 0-len(buff)%8; i++ {
				buff = append(buff, 0)
			}
		}
		buff = append(buff, idxBuff...)
	}
	return writer.Write(buff)
}

// 从一个内存表直接写入SSTable（L0）
func (sst *SSTable) FromMemTable(list *table.SkipList) error {
	sst.file.Seek(0, io.SeekStart)
	first := list.First()
	end := list.End()
	buff := make([]byte, 65536)
	idx := uint64(0)
	md := make(MetaData)
	for p := first.Next(); p != end; p = p.Next() {
		md.AddKey(p.Key().Key(), idx)
		val := p.Val()
		binary.LittleEndian.PutUint32(buff, uint32(len(val)))
		copy(buff[4:], val)
		length := len(val) + 4
		// 8字节对齐
		if length%8 != 0 {
			length += (8 - length%8)
		}
		_, err := sst.file.Write(buff[:length])
		if err != nil {
			return err
		}
		idx += uint64(length)
	}
	_, err := md.WriteTo(sst.file)
	return err
}
