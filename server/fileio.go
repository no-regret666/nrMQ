package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"nrMQ/logger"
	"os"
	"sync"
)

type File struct {
	mu        sync.RWMutex
	filename  string
	node_size int
}

// 先检查该磁盘是否存在该文件，如不存在则需要创建
func NewFile(path_name string) (file *File, fd *os.File, Err string, err error) {
	if !CheckFileOrList(path_name) {
		fd, err = CreateFile(path_name)
		if err != nil {
			Err = "Create file failed"
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return nil, nil, Err, err
		}
	} else {
		fd, err = os.OpenFile(path_name, os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			Err = "Open file failed"
			return nil, nil, Err, err
		}
	}

	file = &File{
		mu:        sync.RWMutex{},
		filename:  path_name,
		node_size: NODE_SIZE,
	}
	return file, fd, "ok", err
}

func CheckFile(path string) (file *File, fd *os.File, Err string, err error) {
	if !CheckFileOrList(path) {
		Err = "NotFile"
		err = errors.New(Err)
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil, nil, Err, err
	} else {
		fd, err = os.OpenFile(path, os.O_RDONLY, 0666)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			Err = "Open file failed"
			return nil, nil, Err, err
		}
	}

	file = &File{
		mu:        sync.RWMutex{},
		filename:  path,
		node_size: NODE_SIZE,
	}

	logger.DEBUG(logger.DLog, "the file is %v\n", file)

	return file, fd, "ok", err
}

// 读取文件，获取该partition的最后一个index
func (f *File) GetIndex(file *os.File) (int64, error) {
	var node Key
	var offset int64
	dataNode := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()

	for {
		//从文件中读取数据块
		n, err := file.ReadAt(dataNode, offset)
		if err != nil {
			if err == io.EOF {
				//读到文件末尾，返回最后一个End_index
				return node.End_index, nil
			}
			//其他错误直接返回
			return -1, err
		}

		//解析数据块
		buf := bytes.NewBuffer(dataNode[:n])
		if err := binary.Read(buf, binary.BigEndian, &node); err != nil {
			return -1, err
		}

		//更新偏移量
		offset += int64(n)
	}
}

func (f *File) WriteFile(file *os.File, node Key, data_msg []byte) bool {
	data_node := &bytes.Buffer{}
	err := binary.Write(data_node, binary.BigEndian, node)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		logger.DEBUG(logger.DError, "%v turn bytes fail\n", node)
		return false
	}

	f.mu.Lock()

	if f.node_size == 0 {
		f.node_size = len(data_node.Bytes())
	}
	file.Write(data_node.Bytes())
	file.Write(data_msg)

	f.mu.Unlock()

	if err != nil {
		return false
	} else {
		return true
	}
}

func (f *File) OpenFileRead() *os.File {
	f.mu.RLock()
	fd, err := os.OpenFile(f.filename, os.O_RDONLY, 0666)
	f.mu.RUnlock()
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil
	}
	return fd
}

func (f *File) FindOffset(file *os.File, index int64) (int64, error) {
	var node Key
	data_node := make([]byte, NODE_SIZE)
	offset := int64(0)
	for {
		logger.DEBUG(logger.DLog, "the file name is %v\n", f.filename)
		f.mu.RLock()
		size, err := file.ReadAt(data_node, offset)
		f.mu.RUnlock()

		if err == io.EOF { //读到文件末尾
			logger.DEBUG(logger.DLog, "read All file,do not find this index\n")
			return index, io.EOF
		}
		if size != NODE_SIZE {
			logger.DEBUG(logger.DLog2, "the size is %v NODE_SIZE is %v\n", size, NODE_SIZE)
			return int64(-1), errors.New("read node size is not NODE_SIZE")
		}
		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, data_node)
		binary.Read(buf, binary.BigEndian, &node)
		if node.End_index < index {
			offset += int64(NODE_SIZE + node.Size)
		} else {
			break
		}
	}

	return offset, nil
}
