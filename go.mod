module github.com/pigeonligh/stupid-base

go 1.13

require (
	vitess.io/vitess v0.0.0
	vitess.io/vitess/examples/are-you-alive v0.0.0-20210109023718-d51a9038b9f9 // indirect
)

replace vitess.io/vitess v0.0.0 => ./vitess
