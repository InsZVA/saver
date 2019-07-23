package sstable

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"sort"
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

var (
	brokenFileErr = errors.New("磁盘文件可能已损坏")
)

const (
	blockSize = 64 * 1024
	l0MaxSize = 128 * 1024 * 1024
)

type SSTable struct {
	file SeqFile
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

func OpenSSTable(filepath string) (*SSTable, error) {
	file, err := os.Open(filepath)
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
[KeyLength32, keyValue...]
[ValLength32, valValue...] blockSize对齐
[KeyLength32, keyValue...]
[ValLength32, valValue...]
...
[keyIdx, keyIdx, keyIdx..] block的第一个Key，idx为block的idx，key直接32KB-8字节
[metalength64..]
...
*/

// 从一个内存表直接写入SSTable（L0）
func (sst *SSTable) FromMemTable(list *table.SkipList) error {
	sst.file.Seek(0, io.SeekStart)
	first := list.First()
	end := list.End()
	buff := make([]byte, blockSize)
	idx := uint64(0)
	written := uint64(0)
	md := make([]uint64, 0)
	for p := first.Next(); p != end; p = p.Next() {
		if int(idx)+len(p.Key().Key())+len(p.Val())+8 >= blockSize {
			_, err := sst.file.Write(buff)
			if err != nil {
				return err
			}
			written += blockSize
			idx = 0
		}
		md = append(md, written+idx)
		key := p.Key().Key()
		binary.LittleEndian.PutUint32(buff[idx:], uint32(len(key)))
		idx += 4
		idx += uint64(copy(buff[idx:], key))
		val := p.Val()
		binary.LittleEndian.PutUint32(buff[idx:], uint32(len(val)))
		idx += 4
		idx += uint64(copy(buff[idx:], val))
	}
	metaLength := uint64(len(md)*8 + 8)
	if idx+metaLength >= blockSize {
		_, err := sst.file.Write(buff)
		if err != nil {
			return err
		}
		written += blockSize
		idx = 0
	}
	// 写入metadata
	idx = blockSize - metaLength
	for _, offset := range md {
		binary.LittleEndian.PutUint64(buff[idx:], offset)
		idx += 8
	}
	// 写入metadata长度
	binary.LittleEndian.PutUint64(buff[blockSize-8:], uint64(len(md)*8)+8)
	_, err := sst.file.Write(buff)
	if err != nil {
		return err
	}
	return nil
}

type SSTReader struct {
	sst  *SSTable
	buff [blockSize]byte
	// buff的开始位置和长度
	start, length uint64
}

func (reader *SSTReader) ReadAt(data []byte, offset int64) (int, error) {
	log.Printf("ReadAt: offset %d, reader.start: %d, reader.length: %d\n", offset, reader.start, reader.length)
	if reader.start <= uint64(offset) && uint64(offset)+uint64(len(data)) <= reader.length {
		return copy(data, reader.buff[offset-int64(reader.start):int(offset)+len(data)]), nil
	}
	n, err := reader.sst.file.ReadAt(reader.buff[:], int64(offset&blockSize))
	if err != nil {
		return 0, err
	}
	reader.start = uint64(offset & blockSize)
	reader.length = uint64(n)
	return reader.ReadAt(data, offset)
}

type Iterator struct {
	key *table.Key
	val []byte
	err error
	// TODO
	end    bool
	offset uint64
	reader *SSTReader
}

func (i *Iterator) Next() bool {
	keyLength := make([]byte, 4)
	n, err := i.reader.ReadAt(keyLength, int64(i.offset))
	if err != nil {
		i.err = err
		return false
	}
	i.offset += uint64(n)
	keySlice := make([]byte, binary.LittleEndian.Uint32(keyLength))
	n, err = i.reader.ReadAt(keySlice, int64(i.offset))
	if err != nil {
		i.err = err
		return false
	}
	i.offset += uint64(n)
	valLength := make([]byte, 4)
	n, err = i.reader.ReadAt(valLength, int64(i.offset))
	if err != nil {
		i.err = err
		return false
	}
	i.offset += uint64(n)
	valSlice := make([]byte, binary.LittleEndian.Uint32(valLength))
	n, err = i.reader.ReadAt(valSlice, int64(i.offset))
	if err != nil {
		i.err = err
		return false
	}
	i.offset += uint64(n)
	k := table.NewKey(keySlice)
	i.key = &k
	i.val = valSlice
	// TODO: if has next
	return true
}

func (reader *SSTReader) ReadItem(offset uint64) *Iterator {
	return &Iterator{reader: reader, offset: offset}
}

func (reader *SSTReader) Find(key table.Key) (*Iterator, error) {
	// TODO: 优化metadata的缓存
	metaLength := make([]byte, 8)
	n, err := reader.ReadAt(metaLength, reader.sst.file.Size()-8)
	if err != nil {
		return nil, err
	}
	if n != 8 {
		return nil, brokenFileErr
	}
	length := binary.LittleEndian.Uint64(metaLength)
	log.Printf("metalength: %d\n", length)
	num := int((length - 8) / 8)
	metaStart := reader.sst.file.Size() - int64(length)
	md := make([]uint64, 0, num)
	buff := make([]byte, 8)
	for i := 0; i < num; i++ {
		_, err = reader.ReadAt(buff, int64(int64(i*8)+metaStart))
		if err != nil {
			return nil, err
		}
		md = append(md, binary.LittleEndian.Uint64(buff))
	}
	log.Printf("metadata: %d %v\n", len(md), md)

	found := sort.Search(num, func(i int) bool {
		it := reader.ReadItem(md[i])
		if !it.Next() {
			return false
		}
		return it.key.Cmp(key) >= 0
	})
	return reader.ReadItem(md[found]), nil
}

func (sst *SSTable) NewReader() *SSTReader {
	return &SSTReader{
		sst: sst,
	}
}
