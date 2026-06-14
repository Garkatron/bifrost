package bifrost

import "core:sync"
import "core:fmt"


MutexQueue :: struct($T: typeid) {
    mutex:    sync.Mutex,
    messages: [dynamic]T,
}

queue_init :: proc(q: ^MutexQueue($T)) {
	q.messages = make([dynamic]T)
	q.mutex = {}
}

queue_destroy :: proc(q: ^MutexQueue($T)) {
	delete(q.messages)
}

queue_push :: proc(q: ^MutexQueue($T), msg: T) {
	sync.mutex_lock(&q.mutex)
	defer sync.mutex_unlock(&q.mutex)
	append(&q.messages, msg)
}

queue_pop_all :: proc(q: ^MutexQueue($T)) -> [dynamic]T {
	sync.mutex_lock(&q.mutex)
	defer sync.mutex_unlock(&q.mutex)

	if len(q.messages) == 0 {
		return nil
	}

	out := q.messages

	q.messages = make([dynamic]T)

	return out
}
