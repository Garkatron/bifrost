package test_server

import "core:time"
import "core:fmt"
import net "../../src"
import sv "../../src/server"
import shared "../shared"
import "core:log"
import bn "core:net"

registry: net.NetRegistry(shared.PacketType, shared.PacketData)
server:   sv.NetServer(shared.PacketType, shared.PacketData)

main :: proc() {
	context.logger = log.create_console_logger()

	registry = net.create_registry(shared.PacketType, shared.PacketData)
	registry.entries[.Message] = net.IPacket(shared.PacketData){
		encode = net.generic_encode(shared.PacketData, shared.Message),
		decode = net.generic_decode(shared.PacketData, shared.Message),
	}

	endpoint := bn.Endpoint{
		address = bn.IP4_Loopback,
		port    = 8000,
	}

	listener, listen_err := bn.listen_tcp(endpoint)
	if listen_err != nil {
		log.error("failed to listen:", listen_err)
		return
	}

	server = sv.server_new(shared.PacketType, shared.PacketData, listener, &registry, net.NetManagerConfig{
		debug           = false,
		max_connections = sv.MAX_CLIENTS,
		packet_delay    = 0,
		timeout_time    = 1000,
	})

	sv.start(&server)
	log.info("server listening on", endpoint)

	running := true
	for running {
		messages := net.queue_pop_all(server.incoming)
		if messages != nil {
			for msg in messages {
				switch payload in msg.packet.payload {
					case shared.Message:
					    p := payload
					    text := string(p.content[:p.length])
					    log.info("received message:", text)

					    sv.send_to(&server, msg.client_id, net.Packet(shared.PacketType, shared.PacketData){
					        header = net.PacketHeader(shared.PacketType){
					            type = .Message,
					        },
					        payload = p,
					})
				}

			}
			delete(messages)
		}
		time.sleep(16 * time.Millisecond)

	}
	sv.destroy(&server)
}
