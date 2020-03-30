package main

import (
	"bufio"
	"bytes"
	"fmt"
	//"io/ioutil"
	"encoding/binary"
	"os"
)

const (
	DATA_HEADER_SIZE  = 4
	FIRST_WORD_OFFSET = 8
	WRONG_POSITION    = -1

	COMPARE_EQUALS  = 0
	COMPARE_LOWER   = -1
	COMPARE_GREATER = 1
)

type Storage interface {
	Add(str string)
	Contains(str string) bool
	Close()
}

type storage struct {
	fileName string
	file     *os.File
	header   fileHeader
	fileSize int64
}

type fileHeader struct {
	LastWordOffset uint32
	LastWordSize   uint32
}

type dataHeader struct {
	WordSize uint32
}

func main() {
	var s *storage = New("test.bin")
	run(s)
}

func run(s *storage) {
	s.OpenFile("test.bin")
	//s.Add("ABC")

	newStr := "bbbb"
	if !s.Contains(newStr) {
		fmt.Println("Not contains, add it")
		s.Add(newStr)
	} else {
		fmt.Println("already contains")
	}

	//s.Add("test1")
	defer s.Close()

}

func New(fileName string) *storage {
	res := &storage{}
	res.file = nil
	return res
}

func (s *storage) OpenFile(fileName string) {
	if s.file == nil {
		f, _ := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
		s.file = f
		fileStat, err := s.file.Stat()
		if err != nil {
			panic(err)
		}

		s.fileSize = int64(fileStat.Size())

		if s.fileSize == 0 {
			s.initHeader()
		} else {
			s.readHeader()
		}
	}
}

func (s *storage) Add(str string) {
	offset, _ := s.findOffsetForString(str)
	fmt.Printf("Add: new offset for str = %v\n", offset)

	if offset == -1 {
		offset = FIRST_WORD_OFFSET
	}

	s.insertIntoFile(offset, []byte(str))

	s.file.Sync()
}

func (s *storage) initHeader() {
	s.header = fileHeader{0, 0}
	binary.Write(s.file, binary.LittleEndian, &s.header)
	s.fileSize = 8
}

func (s *storage) readHeader() {
	h := fileHeader{}
	err := binary.Read(s.file, binary.LittleEndian, &h)
	if err != nil {
		panic(err)
	}
	s.header = h
	fmt.Printf("header offset = %v, len =%v \n", h.LastWordOffset, h.LastWordSize)
}

func (s *storage) insertIntoFile(offset int64, data []byte) {
	dataSize := int64(len(data))
	insertSize := DATA_HEADER_SIZE + dataSize
	s.file.Seek(int64(s.fileSize), 0)

	extendFileSize(s.file, insertSize)

	var copyCount int64 = s.fileSize - offset
	s.copyData(offset, offset+insertSize, copyCount)

	fmt.Printf("insertIntoFile: Data = %x\n", data)

	header := &dataHeader{uint32(dataSize)}
	headerOffset := s.writeDataHeader(header, offset)

	s.file.WriteAt(data, offset+headerOffset)
	s.file.Sync()

	fmt.Printf("insertIntoFile: file size = %v\n", s.fileSize)

	s.header.LastWordOffset += uint32(insertSize)
	if offset == s.fileSize {
		fmt.Println("insertIntoFile: written at end")
		s.header.LastWordSize = uint32(dataSize)
		s.header.LastWordOffset = uint32(offset + DATA_HEADER_SIZE)
	}
	s.fileSize += insertSize

}

func extendFileSize(f *os.File, appendSize int64) {
	dumpBytes := make([]byte, appendSize)
	f.Write(dumpBytes)
}

func (s *storage) copyData(from int64, to int64, copyCount int64) {
	buf := make([]byte, 1)
	buf[0] = 7
	for i := copyCount; i > 0; i-- {
		var dstPos int64 = to + i - 1
		var srcPos int64 = from + i - 1
		s.file.ReadAt(buf, int64(srcPos))
		s.file.WriteAt(buf, int64(dstPos))
	}
}

func (s *storage) writeDataHeader(dataIn *dataHeader, offset int64) int64 {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, dataIn)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	s.file.WriteAt(buf.Bytes(), offset)
	return int64(buf.Len())
}

func (s *storage) findOffsetForString(str string) (int64, bool) {
	fmt.Printf("find offset for '%s'\n", str)
	var offset int64 = 8

	if s.isEmptyFile() {
		fmt.Println("empty file")
		return WRONG_POSITION, false
	}

	givenWordBytes := []byte(str)

	lastWordBytes := s.readWordBySize(int64(s.header.LastWordOffset), int64(s.header.LastWordSize))
	fmt.Printf("Last word: %s\n", string(lastWordBytes))
	compareResult := bytes.Compare(givenWordBytes, lastWordBytes)

	fmt.Printf("Compared with last word result = %v\n", compareResult)
	if compareResult == COMPARE_EQUALS {
		fmt.Println("Found last word matched given")
		return int64(s.header.LastWordOffset), true
	}

	if compareResult == COMPARE_GREATER {
		fmt.Printf("%x is greater then last word %x\n", givenWordBytes, lastWordBytes)
		return int64(s.header.LastWordOffset + s.header.LastWordSize), false
	}

	offset = int64(FIRST_WORD_OFFSET)

	for offset < int64(s.header.LastWordOffset) {
		fmt.Printf("current offset = %v\n", offset)
		currentWordBytes, currentWordSize := s.readWord(offset)

		readedStr := string(currentWordBytes)
		fmt.Printf("readed current str = %s, %x\n", readedStr, currentWordBytes)

		compareResult = bytes.Compare(givenWordBytes, currentWordBytes)
		if compareResult == COMPARE_EQUALS {
			fmt.Println("found in middle")
			return offset, true

		}

		if compareResult == COMPARE_LOWER {
			return offset, false
		}
		offset += int64(currentWordSize + DATA_HEADER_SIZE)

	}

	offset = int64(s.header.LastWordOffset + s.header.LastWordSize)
	return offset, true
}

func (s *storage) isEmptyFile() bool {
	return s.header.LastWordSize == 0
}

func (s *storage) readWordBySize(offset int64, size int64) []byte {
	_, err := s.file.Seek(offset, 0)
	if err != nil {
		panic(err)
	}

	lastWordBytes := make([]byte, s.header.LastWordSize)
	bufr := bufio.NewReader(s.file)
	_, err = bufr.Read(lastWordBytes)

	return lastWordBytes
}

func (s *storage) readWord(offset int64) ([]byte, int64) {
	_, err := s.file.Seek(offset, 0)
	if err != nil {
		panic(err)
	}
	header := &dataHeader{}
	binary.Read(s.file, binary.LittleEndian, header)
	size := int64(header.WordSize)
	return s.readWordBySize(offset+DATA_HEADER_SIZE, size), size
}

func (s *storage) Contains(str string) bool {

	if s.fileSize == 0 {
		fmt.Println("There is no any data in file")
		return false
	}

	_, isFound := s.findOffsetForString(str)
	return isFound
}

func (s *storage) findOffset(str string) (uint32, error) {
	//var uint32 offset = 0
	//reader := bufio.NewReader(s.file)
	h := fileHeader{}
	binary.Read(s.file, binary.LittleEndian, &h)
	fmt.Printf("header offset = %v, len =%v \n", h.LastWordOffset, h.LastWordSize)
	return 0, nil
}

func (s *storage) Close() {
	var buf bytes.Buffer
	fmt.Printf("Header size=%v, offse=%v\n", s.header.LastWordSize, s.header.LastWordOffset)
	binary.Write(&buf, binary.LittleEndian, &s.header)
	written, err := s.file.WriteAt(buf.Bytes(), int64(0))
	if err != nil {
		fmt.Println("Write header error: ", err)
	}
	fmt.Printf("Close: header %x, written %v\n", buf.Bytes(), written)
	s.file.Sync()
	s.file.Close()
}
