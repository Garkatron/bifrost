package bifrost

import "core:fmt"
import "core:encoding/uuid"
import "core:net"
import "core:thread"
import "core:mem"
import "core:time"
import "core:sys/posix"

NetManagerConfig :: struct {
	packet_delay:    int, // milliseconds
	timeout_time:    int, // milliseconds
	max_connections: int,
	debug:           bool, // whether to log debug messages
}

// NetManager is the main network manager struct, handling incoming and outgoing packets.
NetManager :: struct($T: typeid, $D: typeid) {
	config:   NetManagerConfig,
	socket:   net.TCP_Socket,
	registry: ^Registry(T, D),

	seq: u32,

	incoming: ^MutexQueue(NetworkMessage(T, D)), // shared by all threads
	outgoing: ^MutexQueue(OutgoingMessage(T, D)), // private to manager

	thread_recv: ^thread.Thread,
	thread_send: ^thread.Thread,

	running: bool,
	id:      uuid.Identifier, // zero if server
}

net_manager_send :: proc(m: ^NetManager($T, $D), packet: Packet(T, D)) {
	queue_push(m.outgoing, OutgoingMessage(T, D){packet = packet})
}

net_manager_new :: proc(
	$T: typeid, $D: typeid,
	config: NetManagerConfig,
	socket: net.TCP_Socket,
	id: uuid.Identifier,
	registry: ^Registry(T, D),
	incoming: ^MutexQueue(NetworkMessage(T, D)),
) -> ^NetManager(T, D) {
	m := new(NetManager(T, D))
	m.config   = config
	m.socket   = socket
	m.id       = id
	m.seq      = 0
	m.registry = registry
	m.incoming = incoming
	m.outgoing = new(MutexQueue(OutgoingMessage(T, D)))
	m.running  = false
	queue_init(m.outgoing)
	return m
}

net_manager_start :: proc(m: ^NetManager($T, $D)) {
	m.running = true
	m.thread_send = thread.create_and_start_with_data(m, _thread_send_entry(T, D))
	m.thread_recv = thread.create_and_start_with_data(m, _thread_recv_entry(T, D))
}

net_manager_close :: proc(m: ^NetManager($T, $D)) {
	m.running = false
	posix.shutdown(posix.FD(m.socket), .RDWR)
	net.close(m.socket)
}

net_manager_destroy :: proc(m: ^NetManager($T, $D)) {
	if m.running do net_manager_close(m)
	fmt.println("joining send thread...")
	thread.join(m.thread_send)
	fmt.println("send joined")
	thread.join(m.thread_recv)
	fmt.println("recv joined")
	thread.destroy(m.thread_recv)
	thread.destroy(m.thread_send)
	queue_destroy(m.outgoing)
	free(m.outgoing)
	free(m)
}

@(private = "file")
_debug_log :: proc(manager: ^NetManager($T, $D), args: ..any) {
	if !manager.config.debug do return
	id_str := uuid.to_string(manager.id)
	defer delete(id_str)
	fmt.printfln("[NetManager %s]: %v", id_str, args)
}


@(private = "file")
_thread_send_entry :: proc($T: typeid, $D: typeid) -> proc(rawptr) {
	return proc(data: rawptr) {
		manager := (^NetManager(T, D))(data)
		_thread_send(manager)
	}
}

@(private = "file")
_thread_recv_entry :: proc($T: typeid, $D: typeid) -> proc(rawptr) {
	return proc(data: rawptr) {
		manager := (^NetManager(T, D))(data)
		_thread_recv(manager)
	}
}

@(private = "file")
_thread_send :: proc(manager: ^NetManager($T, $D)) {
	for manager.running {
		messages := queue_pop_all(manager.outgoing)
		if messages != nil {
			for &msg in messages {
				// Inject sequence number and timestamp into the packet header
				msg.packet.header.seq  = manager.seq
				msg.packet.header.time = time.to_unix_nanoseconds(time.now())
				manager.seq += 1

				// Encode the packet and send it over the network
				encoded := encode_packet(manager.registry, msg.packet)
				net.send_tcp(manager.socket, encoded)
				delete(encoded)
			}
			delete(messages)
		}
		time.sleep(time.Duration(manager.config.packet_delay) * time.Millisecond)
	}
}

@(private = "file")
_thread_recv :: proc(manager: ^NetManager($T, $D)) {
	defer {
		net.close(manager.socket)
		manager.running = false
		_debug_log(manager, "Thread destroyed")
	}

	for {
		header: PacketHeader(T)
		header_bytes := mem.ptr_to_bytes(&header)
		header_total := 0
		for header_total < len(header_bytes) {
			n, err := net.recv_tcp(manager.socket, header_bytes[header_total:])
			if err != nil || n == 0 do return
			header_total += n
		}

		// Payload size is whatever the sender wrote into header.length,
		// since variable-size packets (slices) no longer have a fixed
		// per-type size.
		payload_size := int(header.length)
		payload_buf := make([]byte, payload_size)
		payload_total := 0
		for payload_total < payload_size {
			n, err := net.recv_tcp(manager.socket, payload_buf[payload_total:])
			if err != nil || n == 0 do return
			payload_total += n
		}

		result, ok := decode_packet(manager.registry, header, payload_buf)
		delete(payload_buf)

		if ok {
			queue_push(manager.incoming, NetworkMessage(T, D){
				client_id = manager.id,
				packet    = result,
			})
			// _debug_log(manager, "Received packet of type", header.type, "with size", payload_size)
		}
	}
}
