package test_client

import "core:time"
import net "../../src"
import cl "../../src/client"
import shared "../shared"
import "core:log"
import bn "core:net"

registry: net.NetRegistry(shared.PacketType, shared.PacketData)
client:   cl.NetClient(shared.PacketType, shared.PacketData)

main :: proc() {
	context.logger = log.create_console_logger()

	endpoint := bn.Endpoint{
		address = bn.IP4_Loopback,
		port    = 8000,
	}

	registry = net.create_registry(shared.PacketType, shared.PacketData)
	registry.entries[.Message] = net.IPacket(shared.PacketData){
		encode = net.generic_encode(shared.PacketData, shared.Message),
		decode = net.generic_decode(shared.PacketData, shared.Message),
	}

	client = cl.client_new(shared.PacketType, shared.PacketData, net.NetManagerConfig{
		debug           = false,
		max_connections = 1,
		packet_delay    = 0,
		timeout_time    = 1000,
	})

	if ok := cl.connect(&client, &registry, endpoint); !ok {
		log.error("failed to connect")
		return
	}

	cl.start(&client)

	msg_text := "hello"
	content_bytes: [shared.MAX_CHAT_LEN]byte
	copy(content_bytes[:], msg_text)

	cl.send(&client, net.Packet(shared.PacketType, shared.PacketData){
		header = net.PacketHeader(shared.PacketType){
			type = .Message,
		},
		payload = shared.Message{
			length  = len(msg_text),
			content = content_bytes,
		},
	})

	running := true
	for running {
		messages := net.queue_pop_all(&client.incoming)
		if messages != nil {
			for msg in messages {
				switch payload in msg.packet.payload {
					case shared.Message:
					    p := payload
					    text := string(p.content[:p.length])
					    log.info("received message:", text)

						running = false
					    cl.send(&client, net.Packet(shared.PacketType, shared.PacketData){
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
	cl.close_and_destroy(&client)
	log.info("finished")
}
