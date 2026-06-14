package server

import bi "../../src"
import "core:net"
import "core:sync"
import "core:thread"
import "core:encoding/uuid"

MAX_CLIENTS :: 1024

NetServer :: struct($T: typeid, $D: typeid) {
	socket:        net.TCP_Socket,
	clients:       map[uuid.Identifier]^bi.NetManager(T, D),
	clients_mutex: sync.Mutex,
	incoming:      ^bi.MutexQueue(bi.NetworkMessage(T, D)),
	registry:      ^bi.Registry(T, D),
	config:        bi.NetManagerConfig,
	thread_accept: ^thread.Thread,
	running:       bool,
}

server_new :: proc($T: typeid, $D: typeid, listener: net.TCP_Socket, registry: ^bi.Registry(T, D), config: bi.NetManagerConfig) -> NetServer(T, D) {
	return NetServer(T, D){
		socket   = listener,
		clients  = make(map[uuid.Identifier]^bi.NetManager(T, D)),
		incoming = new(bi.MutexQueue(bi.NetworkMessage(T, D))),
		registry = registry,
		config   = config,
	}
}

start :: proc(server: ^NetServer($T, $D)) {
	server.running = true
	bi.queue_init(server.incoming)
	server.thread_accept = thread.create_and_start_with_data(server, _thread_accept_entry(T, D))
}

destroy :: proc(server: ^NetServer($T, $D)) {
	server.running = false

	sync.mutex_lock(&server.clients_mutex)
	for id, m in server.clients {
		bi.net_manager_close(m)
		bi.net_manager_destroy(m)
		delete_key(&server.clients, id)
	}
	sync.mutex_unlock(&server.clients_mutex)

	delete(server.clients)
	bi.queue_destroy(server.incoming)
	free(server.incoming)
	net.close(server.socket)
}

send :: proc(server: ^NetServer($T, $D), target: bi.SendTarget, packet: bi.Packet(T, D)) -> bool {
	sync.mutex_lock(&server.clients_mutex)
	defer sync.mutex_unlock(&server.clients_mutex)

	switch t in target {
	case bi.SendClient:
		return _send_to(server, t.id, packet)
	case bi.SendBroadcast:
		_broadcast(server, packet)
		return true
	case bi.SendExcept:
		for id, m in server.clients {
			if id != t.exclude {
				bi.net_manager_send(m, packet)
			}
		}
		return true
	}
	return false
}

broadcast :: proc(server: ^NetServer($T, $D), packet: bi.Packet(T, D)) {
	sync.mutex_lock(&server.clients_mutex)
	defer sync.mutex_unlock(&server.clients_mutex)
	_broadcast(server, packet)
}

send_to :: proc(server: ^NetServer($T, $D), id: uuid.Identifier, packet: bi.Packet(T, D)) -> bool {
	sync.mutex_lock(&server.clients_mutex)
	defer sync.mutex_unlock(&server.clients_mutex)
	return _send_to(server, id, packet)
}

@(private = "file")
_thread_accept_entry :: proc($T: typeid, $D: typeid) -> proc(rawptr) {
	return proc(data: rawptr) {
		server := (^NetServer(T, D))(data)
		_thread_accept(server)
	}
}

@(private = "file")
_thread_accept :: proc(server: ^NetServer($T, $D)) {
	for server.running {
		client, _, err := net.accept_tcp(server.socket)
		if err != nil {
			continue
		}

		client_id := uuid.generate_v4()
		net_manager := bi.net_manager_new(T, D, server.config, client, client_id, server.registry, server.incoming)
		if net_manager == nil {
			net.close(client)
			continue
		}

		bi.net_manager_start(net_manager)

		sync.mutex_lock(&server.clients_mutex)
		server.clients[client_id] = net_manager
		sync.mutex_unlock(&server.clients_mutex)
	}
}

@(private = "file")
_broadcast :: proc(server: ^NetServer($T, $D), packet: bi.Packet(T, D)) {
	for _, m in server.clients {
		bi.net_manager_send(m, packet)
	}
}

@(private = "file")
_send_to :: proc(server: ^NetServer($T, $D), id: uuid.Identifier, packet: bi.Packet(T, D)) -> bool {
	m, ok := server.clients[id]
	if !ok do return false
	bi.net_manager_send(m, packet)
	return true
}
