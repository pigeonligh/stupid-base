/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import (
	"os"

	"github.com/pigeonligh/stupid-base/pkg/core/storage/buffer"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

const bufferSize = 64

// Manager provides a file management
type Manager struct {
	buffer *buffer.Manager

	files map[string]*FileHandle
}

var instance *Manager

// GetInstance returns the instance
func GetInstance() *Manager {
	return instance
}

func init() {
	log.V(log.StorageLevel).Info("Storage Manager starts to initialize.")
	defer log.V(log.StorageLevel).Info("Storage Manager has been initialized.")
	instance = &Manager{
		buffer: buffer.NewManager(bufferSize, types.PageSize),
	}
}

// CreateFile creates a new file
func (m *Manager) CreateFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	file.Close()
	return nil
}

// DestroyFile deletes a file
func (m *Manager) DestroyFile(filename string) error {
	return os.Remove(filename)
}

// OpenFile opens a file
func (m *Manager) OpenFile(filename string) (*FileHandle, error) {
	if file, found := m.files[filename]; found {
		return file, nil
	}
	return newFileHanle(filename, m.buffer)
}

// CloseFile closes a file
func (m *Manager) CloseFile(filename string) error {
	if handle, found := m.files[filename]; found {
		if err := handle.file.Close(); err != nil {
			return err
		}
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
