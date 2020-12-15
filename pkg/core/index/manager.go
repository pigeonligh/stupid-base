package index

import (
	"sync"

	"github.com/pigeonligh/stupid-base/pkg/core/storage"
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
		log.V(log.IndexLavel).Info("Index Manager starts to initialize.")
		defer log.V(log.IndexLavel).Info("Index Manager has been initialized.")
		instance = &Manager{
			storage: storage.GetInstance(),
			files:   make(map[string]*FileHandle),
		}
	})
	return instance
}

func (m *Manager) CreateIndex(filename string) error {
	// TODO
	return nil
}

func (m *Manager) OpenIndex(filename string) (*FileHandle, error) {
	// TODO
	return nil, nil
}

func (m *Manager) CloseIndex(efilename string) error {
	// TODO
	return nil
}

func (m *Manager) DestroyIndex(filename string) error {
	// TODO
	return nil
}
