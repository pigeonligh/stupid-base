module github.com/pigeonligh/stupid-base

go 1.13

require (
	github.com/c-bata/go-prompt v0.2.5
	github.com/golang/protobuf v1.4.0 // indirect
	github.com/mattn/go-colorable v0.1.8 // indirect
	github.com/stretchr/testify v1.6.1 // indirect
	golang.org/x/sys v0.0.0-20201214210602-f9fddec55a1e // indirect
	vitess.io/vitess v0.0.0
)

replace vitess.io/vitess v0.0.0 => ./vitess
