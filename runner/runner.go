package runner

type Runner interface {
	Start() error
	Close() error
}
