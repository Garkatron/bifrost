package bifrost

import "core:log"
import "core:encoding/uuid"
import "core:net"
import "core:thread"
import "core:time"
import enet "vendor:ENet"
import "core:sync"
import "core:os"
import "core:math/rand"
import "core:strings"
import "core:fmt"




Server :: struct($T: typeid, $D: typeid) {
	config:   Config,

	registry: ^NetRegistry(T, D),
	logger:    Maybe(log.Logger),

	next_id: u32,

	clients_mutex: sync.Mutex,
	client_map: map[u32]ClientData,

	host: enet.Host,

	seq: u32,

	incoming: ^MutexQueue(NetworkMessage(T, D)),
	outgoing: ^MutexQueue(OutgoingMessage(T, D)),

	thread_enet: ^thread.Thread,

	running: bool,
}


net_manager_new :: proc(
	$T: typeid, $D: typeid,
	config: Config,
	id: uuid.Identifier,
	registry: ^NetRegistry(T, D),
	incoming: ^MutexQueue(NetworkMessage(T, D)),
	logger := context.logger
) -> ^Server(T, D) {
	m := new(Server(T, D))
	m.config   = config
	m.seq      = 0
	m.registry = registry
	m.incoming = incoming
	m.outgoing = new(MutexQueue(OutgoingMessage(T, D)))
	m.running  = false
	m.logger   = logger
	queue_init(m.outgoing)
	return m
}

server_start :: proc(m: ^Server($T, $D)) {
	if m.logger != nil {
		context.logger = m.logger
	}

	if enet.initialize() != 0 {
		log.error("Failed to create ENet server host.")
		os.exit(1)
	}

	m.running = true
	m.host = enet.host_create(&m.config.address, m.config.max_connections, 6, m.config.incoming_bandwidth, m.config.outgoing_bandwidth)
	m.thread_enet = thread.create_and_start_with_data(m, _thread_enet_entry(T, D))
}

server_close :: proc(m: ^Server($T, $D)) {
	// TODO: transport teardown
	m.running = false
}

server_destroy :: proc(m: ^Server($T, $D)) {
	if m.running do server_close(m)
	thread.join(m.thread_enet)
	thread.destroy(m.thread_enet)
	queue_destroy(m.outgoing)
	free(m.outgoing)
	free(m)
}

@(private = "file")
_thread_enet_entry :: proc($T: typeid, $D: typeid) -> proc(rawptr) {
	return proc(data: rawptr) {
		manager := (^Server(T, D))(data)
		_thread_enet_loop(manager)
	}
}

_thread_enet_loop :: proc(manager: ^Server($T, $D)) {
	for manager.running {
		event: enet.Event
		result := enet.host_service(manager.host, &event, manager.config.packet_delay)

		messages := queue_pop_all(manager.outgoing)
		if messages != nil {
			for &msg in messages {
				msg.packet.header.seq  = manager.seq
				msg.packet.header.time = time.to_unix_nanoseconds(time.now())
				manager.seq += 1

				encoded := encode_packet(manager.registry, msg.packet)
				if msg.peer != nil {
					_send_raw(manager, msg.peer, encoded)
				} else {
					_broadcast_raw(manager, encoded, msg.exclude)
				}
				delete(encoded)
				free_packet_payload(manager.registry, msg.packet)
			}
			delete(messages)
		}

		if result < 0 do break
		if result == 0 do continue

		switch event.type {
		case .CONNECT:    handle_connect(manager, &event)
		case .RECEIVE:
			handle_receive(manager, &event)
			enet.packet_destroy(event.packet)
		case .DISCONNECT: handle_disconnect(manager, &event)
		}
	}
}

handle_connect :: proc(m: ^Server($T, $D), event: ^enet.Event) {
    id  := next_id(m)
    uid := uuid.generate_v4()
    m.client_map[id] = ClientData{id = id, uid = uid, peer = event.peer}

    hs := HandshakePayload{id = id, uid = uid}
    packet := enet.packet_create(&hs, size_of(HandshakePayload), {.RELIABLE})
    enet.peer_send(event.peer, BIFROST_SYSTEM_CHANNEL, packet) // channel 0
}


handle_receive :: proc(m: ^Server($T, $D), event: ^enet.Event) {
	incoming_data := event.packet.data[:event.packet.dataLength]
	header := (^PacketHeader(T))(raw_data(incoming_data))^

	client_id: u32
	for id, c in m.client_map {
		if c.peer == event.peer {
			client_id = id
			break
		}
	}

	if result, ok := decode_packet(m.registry, header, incoming_data); ok {
		queue_push(m.incoming, NetworkMessage(T, D){
			client_id = client_id,
			packet    = result,
		})
	}
}

handle_disconnect :: proc(m: ^Server($T, $D), event: ^enet.Event) {
	sync.mutex_lock(&m.clients_mutex)
	defer sync.mutex_unlock(&m.clients_mutex)
	if event.peer.connectID in m.client_map {
		name := m.client_map[event.peer.connectID].name
		delete_key(&m.client_map, event.peer.connectID)
	}
}

@(private = "file")
_send_raw :: proc(m: ^Server($T, $D), peer: ^enet.Peer, data: []byte) {
    packet := enet.packet_create(raw_data(data), len(data), {.RELIABLE})
    enet.peer_send(peer, BIFROST_USER_CHANNEL_START, packet) // channel 1
}

@(private = "file")
_broadcast_raw :: proc(m: ^Server($T, $D), data: []byte, exclude: u32 = 0) {
	packet := enet.packet_create(raw_data(data), len(data), {.RELIABLE})
	for id, c in m.client_map {
		if id == exclude do continue
		enet.peer_send(c.peer, BIFROST_SYSTEM_CHANNEL, packet)
	}
}

server_send :: proc {
	server_send_to_id,
	server_send_to_uid,
}

server_send_to_id :: proc(m: ^Server($T, $D), id: u32, packet: Packet(T, D)) -> bool {
	c, ok := m.client_map[id]
	if !ok do return false
	encoded := encode_packet(m.registry, packet)
	defer delete(encoded)
	_send_raw(m, c.peer, encoded)
	return true
}

server_send_to_uid :: proc(m: ^Server($T, $D), uid: uuid.Identifier, packet: Packet(T, D)) -> bool {
	for _, c in m.client_map {
		if c.uid == uid {
			encoded := encode_packet(m.registry, packet)
			defer delete(encoded)
			_send_raw(m, c.peer, encoded)
			return true
		}
	}
	return false
}

server_send_to_peer :: proc(m: ^Server($T, $D), peer: ^enet.Peer, packet: Packet(T, D)) {
	encoded := encode_packet(m.registry, packet)
	defer delete(encoded)
	_send_raw(m, peer, encoded)
}

server_broadcast :: proc(m: ^Server($T, $D), packet: Packet(T, D), exclude: u32 = 0) {
	encoded := encode_packet(m.registry, packet)
	defer delete(encoded)
	_broadcast_raw(m, encoded, exclude)
}

name_is_valid :: proc(name: string) -> bool {
	for c in name {
		if !(('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')) {
			return false
		}
	}
	return true
}

@(private="file") next_id :: proc(m: ^Server($T, $D)) -> u32 {
	m.next_id += 1
	return m.next_id
}
