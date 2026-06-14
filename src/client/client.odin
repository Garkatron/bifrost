package client

import bi "../../src"
import "core:fmt"
import "core:thread"
import "core:mem"
import "core:net"
import "core:time"
import "core:encoding/uuid"

NetClient :: struct($T: typeid, $D: typeid) {
	socket:   net.TCP_Socket,
	manager:  ^bi.NetManager(T, D),
	config:   bi.NetManagerConfig,
	incoming: bi.MutexQueue(bi.NetworkMessage(T, D)),
}

client_new :: proc($T: typeid, $D: typeid, config: bi.NetManagerConfig) -> NetClient(T, D) {
	net_client := NetClient(T, D){
		config = config,
	}
	bi.queue_init(&net_client.incoming)
	return net_client
}

connect :: proc(client: ^NetClient($T, $D), registry: ^bi.Registry(T, D), endpoint: net.Endpoint) -> bool {
	socket, dial_err := net.dial_tcp(endpoint)
	if dial_err != nil {
		// debug.log("connection error (is the server running?):", dial_err)
		return false
	}
	client.socket = socket
	client.manager = bi.net_manager_new(T, D, client.config, socket, uuid.Identifier{}, registry, &client.incoming)
	// debug.log("connected to server")
	return true
}

start :: proc(client: ^NetClient($T, $D)) {
	if client.manager == nil {
		// debug.log("manager is nil")
		return
	}
	bi.net_manager_start(client.manager)
}

client_flush :: proc(client: ^NetClient($T, $D)) {
	for len(client.manager.outgoing.messages) > 0 {
		time.sleep(1 * time.Millisecond)
	}
}

client_close :: proc(client: ^NetClient($T, $D)) {
	if client.manager == nil {
		// debug.log("manager is nil")
		return
	}
	if client.manager.running {
		bi.net_manager_close(client.manager)
	}
}

close_and_destroy :: proc(client: ^NetClient($T, $D)) {
	if client.manager == nil {
		// debug.log("manager is nil")
		return
	}
	if client.manager.running {
		bi.net_manager_close(client.manager)
	}
	bi.net_manager_destroy(client.manager)
	bi.queue_destroy(&client.incoming)
}

send :: proc(client: ^NetClient($T, $D), packet: bi.Packet(T, D)) -> bool {
	if client.manager == nil {
		// debug.log("manager is nil")
		return false
	}
	bi.net_manager_send(client.manager, packet)
	return true
}
