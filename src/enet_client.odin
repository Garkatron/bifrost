package bifrost

import "core:log"
import "core:thread"
import "core:time"
import enet "vendor:ENet"

Client :: struct($T: typeid, $D: typeid) {
	config:   Config,
	registry: ^NetRegistry(T, D),
	logger:   Maybe(log.Logger),

	host: ^enet.Host,
	peer: ^enet.Peer,

	seq: u32,

	incoming: ^MutexQueue(NetworkMessage(T, D)),
	outgoing: ^MutexQueue(OutgoingMessage(T, D)),

	thread_enet: ^thread.Thread,
	running:     bool,
}

client_new :: proc(
	$T: typeid, $D: typeid,
	config: Config,
	registry: ^NetRegistry(T, D),
	incoming: ^MutexQueue(NetworkMessage(T, D)),
	logger := context.logger,
) -> ^Client(T, D) {
	m := new(Client(T, D))
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

client_start :: proc(m: ^Client($T, $D)) -> bool {
	if m.logger != nil do context.logger = m.logger.?

	if enet.initialize() != 0 {
		log.error("Failed to initialize ENet.")
		return false
	}

	m.host = enet.host_create(nil, 1, m.config.max_connections, m.config.incoming_bandwidth, m.config.outgoing_bandwidth)
	if m.host == nil {
		log.error("Failed to create ENet client host.")
		return false
	}

	address: enet.Address
	enet.address_set_host(&address, m.config.host)
	address.port = m.config.port

	m.peer = enet.host_connect(m.host, &address, m.config.max_connections, 0)
	if m.peer == nil {
		log.error("Failed to connect to server.")
		return false
	}

	m.running = true
	m.thread_enet = thread.create_and_start_with_data(m, _client_thread_entry(T, D))
	return true
}

client_send :: proc(m: ^Client($T, $D), packet: Packet(T, D)) {
	queue_push(m.outgoing, OutgoingMessage(T, D){packet = packet, peer = m.peer})
}

client_close :: proc(m: ^Client($T, $D)) {
	m.running = false
	if m.peer != nil do enet.peer_disconnect(m.peer, 0)
}

client_destroy :: proc(m: ^Client($T, $D)) {
	if m.running do client_close(m)
	thread.join(m.thread_enet)
	thread.destroy(m.thread_enet)
	if m.host != nil do enet.host_destroy(m.host)
	enet.deinitialize()
	queue_destroy(m.outgoing)
	free(m.outgoing)
	free(m)
}

@(private = "file")
_client_thread_entry :: proc($T: typeid, $D: typeid) -> proc(rawptr) {
	return proc(data: rawptr) {
		m := (^Client(T, D))(data)
		_client_thread_loop(m)
	}
}

@(private = "file")
_client_thread_loop :: proc(m: ^Client($T, $D)) {
	for m.running {
		event: enet.Event
		result := enet.host_service(m.host, &event, m.config.packet_delay)

		// Drain outgoing
		messages := queue_pop_all(m.outgoing)
		if messages != nil {
			for &msg in messages {
				msg.packet.header.seq  = m.seq
				msg.packet.header.time = time.to_unix_nanoseconds(time.now())
				m.seq += 1

				encoded := encode_packet(m.registry, msg.packet)
				_client_send_raw(m, encoded)
				delete(encoded)
				free_packet_payload(m.registry, msg.packet)
			}
			delete(messages)
		}

		if result < 0 do break
		if result == 0 do continue

		switch event.type {
		case .CONNECT:
			log.infof("Connected to server.")
		case .RECEIVE:
			_client_handle_receive(m, &event)
			enet.packet_destroy(event.packet)
		case .DISCONNECT:
			log.infof("Disconnected from server.")
			m.running = false
		}
	}
}

@(private = "file")
_client_send_raw :: proc(m: ^Client($T, $D), data: []byte) {
	packet := enet.packet_create(raw_data(data), len(data), {.RELIABLE})
	enet.peer_send(m.peer, BIFROST_USER_CHANNEL_START, packet)
}

_client_handle_system :: proc(m: ^Client($T, $D), event: ^enet.Event) {
    if event.packet.dataLength < size_of(SystemPacketType) do return
    type := (^SystemPacketType)(event.packet.data)^
    switch type {
    case .HANDSHAKE:
        hs := (^HandshakePayload)(event.packet.data)^
        m.id  = hs.id
        m.uid = hs.uid
        log.infof("Assigned id=%v", m.id)
    case .PING:
        pong := SystemPacketType.PONG
        packet := enet.packet_create(&pong, size_of(SystemPacketType), {.RELIABLE})
        enet.peer_send(m.peer, BIFROST_SYSTEM_CHANNEL, packet)
    }
}

@(private = "file")
_client_handle_receive :: proc(m: ^Client($T, $D), event: ^enet.Event) {
    if event.channelID == BIFROST_SYSTEM_CHANNEL {
        _client_handle_system(m, event)
        return
    }
    // channel 1+ -- packets
    incoming_data := event.packet.data[:event.packet.dataLength]
    header := (^PacketHeader(T))(raw_data(incoming_data))^
    if result, ok := decode_packet(m.registry, header, incoming_data); ok {
        queue_push(m.incoming, NetworkMessage(T, D){
            client_id = 0,
            packet    = result,
        })
    }
}
