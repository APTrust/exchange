package app

type Logger interface {
	Output(maxdepth int, s string) error
}
