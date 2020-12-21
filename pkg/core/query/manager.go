package query

import (
	"sync"

	"github.com/pigeonligh/stupid-base/pkg/core/dbsys"
	"github.com/pigeonligh/stupid-base/pkg/core/index"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

type Manager struct {
	sys    *dbsys.Manager
	index  *index.Manager
	record *record.Manager
}

var instance *Manager
var once sync.Once

func GetInstance() *Manager {
	once.Do(func() {
		log.V(log.QueryLanguageLevel).Info("QueryLanguage Manager starts to initialize.")
		defer log.V(log.QueryLanguageLevel).Info("QueryLanguage Manager has been initialized.")
		instance = &Manager{
			record: record.GetInstance(),
			index:  index.GetInstance(),
			sys:    dbsys.GetInstance(),
		}
	})
	return instance
}
