package record

import (
	"unsafe"

	"github.com/pigeonligh/stupid-base/pkg/core/storage"
)

type Manager struct {
	storage *storage.Manager
	files   map[string]*FileHandle
}

var instance *Manager

func GetInstance() *Manager {
	return instance
}
func init() {
	instance = &Manager{
		storage: storage.GetInstance(),
	}
}

func (m *Manager) CreateFile(filename string, recordSize uint32) error {
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
	header := (*Header)(unsafe.Pointer(&pageHandle.Data))
	header.RecordSize = recordSize
	header.PageNum = 1
	header.RecordNum = 0
	header.RecordPerPage = recordPerPage(recordSize)
	header.SlotMapSize = bitMapSize(header.RecordPerPage)
	header.SizeOfHeader = header.SlotMapSize + 4 // equals to sizeof(PageNum)
	header.FirstSparePage = 0

	if err = fileHandle.MarkDirty(pageHandle.Page); err != nil {
		return err
	}
	if err = fileHandle.UnpinPage(pageHandle.Page); err != nil {
		return nil
	}
	return nil
}

func (m *Manager) DestroyFile(filename string) error {
	if err := m.storage.DestroyFile(filename); err != nil {
		return err
	}
	return nil
}

func (m *Manager) OpenFile(filename string) (*FileHandle, error) {

	if file, found := m.files[filename]; found {
		return file, nil
	}

	storageFH, err := m.storage.OpenFile(filename)
	if err != nil {
		return nil, err
	}
	pageHandle, err := storageFH.GetPage(0)
	if err != nil {
		return nil, err
	}

	// RM_FileHandle
	file := fileHandle()
	header := (*Header)(unsafe.Pointer(&pageHandle.Data))
	file.header = *header
	file.headerModified = false
	file.initialized = true
	file.filename = filename

	if err = storageFH.UnpinPage(pageHandle.Page); err != nil {
		return nil, err
	}

	m.files[filename] = file
	return file, nil
}

func (m *Manager) CloseFile(filename string) error {
	if handle, found := m.files[filename]; found {
		if err := handle.Close(); err != nil {
			return err
		}
		delete(m.files, filename)
	}
	return nil
}
