/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import (
	"os"
	"sync"

	"github.com/pigeonligh/stupid-base/pkg/core/env"
	"github.com/pigeonligh/stupid-base/pkg/core/storage/buffer"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

const bufferSize = 65536

// Manager provides a file management
type Manager struct {
	buffer *buffer.Manager

	files map[string]*FileHandle
}

var instance *Manager
var once sync.Once

// GetInstance returns the instance
func GetInstance() *Manager {
	once.Do(func() {
		log.V(log.StorageLevel).Info("Storage Manager starts to initialize.")
		defer log.V(log.StorageLevel).Info("Storage Manager has been initialized.")
		instance = &Manager{
			buffer: buffer.NewManager(bufferSize, types.PageSize),
			files:  make(map[string]*FileHandle),
		}
	})
	return instance
}

//func init() {
//	log.V(log.StorageLevel).Info("Storage Manager starts to initialize.")
//	defer log.V(log.StorageLevel).Info("Storage Manager has been initialized.")
//	instance = &Manager{
//		buffer: buffer.NewManager(bufferSize, types.PageSize),
//		files:  make(map[string]*FileHandle),
//	}
//}

// CreateFile creates a new file
func (m *Manager) CreateFile(filename string) error {
	file, err := os.Create(env.WorkDir + "/" + filename)
	if err != nil {
		return err
	}
	file.Close()
	return nil
}

// DestroyFile deletes a file
func (m *Manager) DestroyFile(filename string) error {
	return os.Remove(env.WorkDir + "/" + filename)
}

// OpenFile opens a file
func (m *Manager) OpenFile(filename string) (*FileHandle, error) {
	if file, found := m.files[filename]; found {
		return file, nil
	}
	handle, err := fileHandle(filename, m.buffer)
	if err != nil {
		return nil, err
	}
	m.files[filename] = handle
	return handle, err
}

// CloseFile closes a file
func (m *Manager) CloseFile(filename string) error {
	if handle, found := m.files[filename]; found {
		m.buffer.FlushPages(handle.file)
		if err := handle.file.Close(); err != nil {
			return err
		}
		delete(m.files, filename)
		handle.buffer = nil
		handle.file = nil
		return nil
	}
	return nil
}

// GetBuffer gets the buffer manager
func (m *Manager) GetBuffer() *buffer.Manager {
	return m.buffer
}
