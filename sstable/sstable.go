package sstable

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"sort"

	"github.com/InsZVA/saver/table"
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
type Writer struct {
	sst     *SSTable
	md      []uint64
	idx     uint64
	buff    [blockSize]byte
	written uint64
}

func (sst *SSTable) NewWriter() *Writer {
	sst.file.Seek(0, io.SeekStart)
	return &Writer{
		sst: sst,
		md:  make([]uint64, 0),
	}
}

func (writer *Writer) Flush() error {
	_, err := writer.sst.file.Write(writer.buff[:])
	if err != nil {
		return err
	}
	writer.written += blockSize
	writer.idx = 0
	return nil
}

func (writer *Writer) Write(key table.Key, val []byte) error {
	if int(writer.idx)+len(key.Key())+len(val) >= blockSize {
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	writer.md = append(writer.md, writer.written+writer.idx)
	binary.LittleEndian.PutUint32(writer.buff[writer.idx:], uint32(len(key.Key())))
	writer.idx += 4
	writer.idx += uint64(copy(writer.buff[writer.idx:], key.Key()))
	binary.LittleEndian.PutUint32(writer.buff[writer.idx:], uint32(len(val)))
	writer.idx += 4
	writer.idx += uint64(copy(writer.buff[writer.idx:], val))
	return nil
}

func (writer *Writer) Done() error {
	metaLength := uint64(len(writer.md)*8 + 8)
	if writer.idx+metaLength >= blockSize {
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	// 写入metadata
	writer.idx = blockSize - metaLength
	for _, offset := range writer.md {
		binary.LittleEndian.PutUint64(writer.buff[writer.idx:], offset)
		writer.idx += 8
	}
	// 写入metadata长度
	binary.LittleEndian.PutUint64(writer.buff[blockSize-8:], uint64(len(writer.md)*8)+8)
	_, err := writer.sst.file.Write(writer.buff[:])
	if err != nil {
		return err
	}
	return nil
}

// 从一个内存表直接写入SSTable（L0）
func (sst *SSTable) FromMemTable(list *table.SkipList) error {
	writer := sst.NewWriter()
	for p := list.First().Next(); p != list.End(); p = p.Next() {
		if err := writer.Write(p.Key(), p.Val()); err != nil {
			return err
		}
	}
	return writer.Done()
}

type SSTReader struct {
	sst  *SSTable
	buff [blockSize]byte
	// buff的开始位置和长度
	start, length uint64
	meta          MetaData
}

func (reader *SSTReader) ReadAt(data []byte, offset int64) (int, error) {
	//log.Printf("ReadAt: offset %d, reader.start: %d, reader.length: %d\n", offset, reader.start, reader.length)
	if reader.start <= uint64(offset) && uint64(offset)+uint64(len(data)) <= reader.start+reader.length {
		return copy(data, reader.buff[offset-int64(reader.start):int(offset)-int(reader.start)+len(data)]), nil
	}

	if reader.start <= uint64(offset) && uint64(offset) < reader.start+reader.length && uint64(offset)+uint64(len(data)) > reader.start+reader.length {
		n := copy(data, reader.buff[offset-int64(reader.start):offset-int64(reader.start)+int64(reader.length)])
		data = data[n:]
		offset += int64(n)
	}
	n, err := reader.sst.file.ReadAt(reader.buff[:], int64(offset-offset%blockSize))
	if err != nil {
		return 0, err
	}
	reader.start = uint64(offset - offset%blockSize)
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
	if i.offset > i.reader.meta.KeyIdx[len(i.reader.meta.KeyIdx)-1] {
		return false
	}
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
	return true
}

func (reader *SSTReader) ReadItem(offset uint64) *Iterator {
	return &Iterator{reader: reader, offset: offset}
}

type MetaData struct {
	MetaStart uint64
	KeyNum    int
	KeyIdx    []uint64
}

func (reader *SSTReader) readMeta() error {
	metaLength := make([]byte, 8)
	n, err := reader.ReadAt(metaLength, reader.sst.file.Size()-8)
	if err != nil {
		return err
	}
	if n != 8 {
		return brokenFileErr
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
			return err
		}
		md = append(md, binary.LittleEndian.Uint64(buff))
	}
	reader.meta.KeyIdx = md
	reader.meta.KeyNum = num
	reader.meta.MetaStart = uint64(metaStart)
	return nil
}

func (reader *SSTReader) Find(key table.Key) (*Iterator, error) {
	num, md := reader.meta.KeyNum, reader.meta.KeyIdx

	found := sort.Search(num, func(i int) bool {
		it := reader.ReadItem(md[i])
		if !it.Next() {
			return false
		}
		return it.key.Cmp(key) >= 0
	})
	return reader.ReadItem(md[found]), nil
}

func (sst *SSTable) NewReader() (*SSTReader, error) {
	reader := &SSTReader{
		sst: sst,
	}
	return reader, reader.readMeta()
}
