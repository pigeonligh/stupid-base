package pkg

import (
	"runtime"
	"strings"
)

// ProjectPath is the path of this project
var ProjectPath string

func init() {
	_, file, _, _ := runtime.Caller(0)
	dirs := strings.Split(file, "/")
	ProjectPath = strings.Join(dirs[:len(dirs)-2], "/") + "/"
}
