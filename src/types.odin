package bifrost

import "core:encoding/uuid"
import enet "vendor:ENet"

BIFROST_SYSTEM_CHANNEL :: 0
BIFROST_USER_CHANNEL_START :: 1

ClientID :: u32
UniqueID :: uuid.Identifier

ClientData :: struct {
	peer: ^enet.Peer,
	id: ClientID,
	uid: UniqueID
}

NetRegistry :: struct($T: typeid, $V: typeid) {
    entries: [T]IPacket(V),
}

NetworkMessage :: struct($T: typeid, $D: typeid) {
    client_id: ClientID,
    packet: Packet(T, D),
}

OutgoingMessage :: struct($T: typeid, $D: typeid) {
	packet:  Packet(T, D),
	peer:    ^enet.Peer, // nil = broadcast
	exclude: u32,
}

Config :: struct {
	packet_delay:    u32, // milliseconds
	timeout_time:    u32, // milliseconds
	max_connections: uint,
	using address: enet.Address,
	incoming_bandwidth: u32,
	outgoing_bandwidth: u32,
}

SystemPacketType :: enum u8 {
    HANDSHAKE = 0,
    PING,
    PONG,
}

HandshakePayload :: struct {
    type: SystemPacketType, // always .HANDSHAKE
    id:   u32,
    uid:  uuid.Identifier,
}
