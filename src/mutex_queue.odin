package bifrost

import "core:sync"
import "core:fmt"


MutexQueue :: struct($T: typeid) {
    mutex:    sync.Mutex,
    values: [dynamic]T,
}

queue_init :: proc(q: ^MutexQueue($T)) {
	q.values = make([dynamic]T)
	q.mutex = {}
}

queue_destroy :: proc(q: ^MutexQueue($T)) {
	delete(q.values)
}

queue_push :: proc(q: ^MutexQueue($T), msg: T) {
	sync.mutex_lock(&q.mutex)
	defer sync.mutex_unlock(&q.mutex)
	append(&q.values, msg)
}

queue_pop_all :: proc(q: ^MutexQueue($T)) -> [dynamic]T {
	sync.mutex_lock(&q.mutex)
	defer sync.mutex_unlock(&q.mutex)

	if len(q.values) == 0 {
		return nil
	}

	out := q.values

	q.values = make([dynamic]T)

	return out
}
