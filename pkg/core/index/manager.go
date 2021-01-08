package index

import (
	"sync"

	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/storage"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

type Manager struct {
	storage *storage.Manager
	files   map[string]*FileHandle
}

var instance *Manager
var once sync.Once

func GetInstance() *Manager {
	once.Do(func() {
		log.V(log.IndexLevel).Info("Index Manager starts to initialize.")
		defer log.V(log.IndexLevel).Info("Index Manager has been initialized.")
		instance = &Manager{
			storage: storage.GetInstance(),
			files:   make(map[string]*FileHandle),
		}
	})
	return instance
}

func (m *Manager) CreateIndex(filename string, attr types.AttrSet) error {
	var err error
	if err = m.storage.CreateFile(filename); err != nil {
		return err
	}
	_, err = NewOperator(filename, nil, &attr)
	return err
}

func (m *Manager) OpenIndex(filename string, record *record.FileHandle) (*FileHandle, error) {
	if file, found := m.files[filename]; found {
		// TODO: should return warn for open file which is opened
		log.V(log.IndexLevel).Infof("OpenFile: %v has already opened! ", filename)
		return file, nil
	}
	// IM_FileHandle
	log.V(log.IndexLevel).Debug("Load Operator")
	oper, err := LoadOperator(filename, record)
	if err != nil {
		return nil, err
	}
	log.V(log.IndexLevel).Debug("Create FileHandle")
	handle, err := NewFileHandle(oper)
	if err != nil {
		return nil, err
	}
	log.V(log.IndexLevel).Infof("OpenFile succeeded: %v", filename)
	m.files[filename] = handle
	return handle, nil
}

func (m *Manager) CloseIndex(filename string) error {
	if handle, found := m.files[filename]; found {
		if err := handle.Close(); err != nil {
			return err
		}
		delete(m.files, filename)
		log.V(log.IndexLevel).Infof("CloseFile succeeded: %v", filename)
		return m.storage.CloseFile(filename)
	}
	return nil
}

func (m *Manager) DestroyIndex(filename string) error {
	if _, found := m.files[filename]; found {
		// TODO: should return warn for open file which is opened
		log.V(log.IndexLevel).Warningf("DestroyFile failed: %v, file opened!", filename)
		return nil
	}
	if err := m.storage.DestroyFile(filename); err != nil {
		return err
	}
	log.V(log.IndexLevel).Infof("DestroyFile succeeded: %v", filename)
	return nil
}
