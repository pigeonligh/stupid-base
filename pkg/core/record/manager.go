package record

import (
	"sync"

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
		log.V(log.RecordLevel).Info("Record Manager starts to initialize.")
		defer log.V(log.RecordLevel).Info("Record Manager has been initialized.")
		instance = &Manager{
			storage: storage.GetInstance(),
			files:   make(map[string]*FileHandle),
		}
	})
	return instance
}

//func init() {
//	instance = &Manager{
//		storage: storage.GetInstance(),
//		files: make(map[string]*FileHandle),
//	}
//}

func (m *Manager) CreateFile(filename string, recordSize int) error {
	var err error
	if err = m.storage.CreateFile(filename); err != nil {
		return err
	}
	fileHandle, err := m.storage.OpenFile(filename)
	if err != nil {
		return err
	}
	// create the header page
	pageHandle, err := fileHandle.NewPage(0)
	if err != nil {
		return err
	}
	// set up the header page
	header := (*types.RecordHeaderPage)(types.ByteSliceToPointer(pageHandle.Data))
	header.RecordSize = recordSize
	header.RecordNum = 0
	header.RecordPerPage = recordPerPage(recordSize)

	header.FileHeaderPage.Pages = 1
	header.FileHeaderPage.FirstFree = 0

	if err = fileHandle.MarkDirty(pageHandle.Page); err != nil {
		return err
	}
	if err = fileHandle.UnpinPage(pageHandle.Page); err != nil {
		return err
	}
	if err = m.storage.CloseFile(filename); err != nil {
		return err
	}

	return nil
}

func (m *Manager) OpenFile(filename string) (*FileHandle, error) {

	if file, found := m.files[filename]; found {
		// TODO: should return warn for open file which is opened
		log.V(log.RecordLevel).Infof("OpenFile: %v has already opened! ", filename)
		return file, nil
	}
	// RM_FileHandle
	file, err := NewFileHandle(filename)
	if err != nil {
		return nil, err
	}
	log.V(log.RecordLevel).Infof("OpenFile succeeded: %v", filename)
	m.files[filename] = file
	return file, nil
}

func (m *Manager) DestroyFile(filename string) error {
	if _, found := m.files[filename]; found {
		// TODO: should return warn for open file which is opened
		log.V(log.RecordLevel).Warningf("DestroyFile failed: %v, file opened!", filename)
		return nil
	}
	if err := m.storage.DestroyFile(filename); err != nil {
		return err
	}
	log.V(log.RecordLevel).Infof("DestroyFile succeeded: %v", filename)
	return nil
}

func (m *Manager) CloseFile(filename string) error {
	if handle, found := m.files[filename]; found {
		if err := handle.Close(); err != nil {
			return err
		}
		delete(m.files, filename)
		log.V(log.RecordLevel).Infof("CloseFile succeeded: %v", filename)
		return m.storage.CloseFile(filename)
	}
	return nil
}
