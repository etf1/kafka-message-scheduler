package store

type Event interface {
	String() string
	Timestamp() int64
}

type Error struct {
	Event
}

type Store interface {
	Events() chan Event
}
