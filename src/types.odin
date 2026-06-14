package bifrost

import "core:encoding/uuid"


NetRegistry :: struct($T: typeid, $V: typeid) {
    entries: [T]IPacket(V),
}

NetworkMessage :: struct($T: typeid, $D: typeid) {
    client_id: uuid.Identifier,
    packet: Packet(T, D),
}

OutgoingMessage :: struct($T: typeid, $D: typeid) {
    packet: Packet(T, D),
}

NetworkContext :: struct($T: typeid, $D: typeid) {
    send_to:   proc(client_id: uuid.Identifier, packet: Packet(T, D)),
    broadcast: proc(packet: Packet(T, D)),
}

SendTarget :: enum {
	SendClient,
	SendBroadcast,
	SendExcept
}
