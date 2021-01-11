package env

const DatabaseDir string = "STUPID-BASE-DATA"

var WorkDir string

func SetWorkDir(wd string) {
	WorkDir = wd
}
