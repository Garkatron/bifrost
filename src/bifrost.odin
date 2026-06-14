package bifrost

create_registry :: proc($T: typeid, $V: typeid) -> NetRegistry(T, V) {
	return NetRegistry(T, V){}
}

// registry := bifrost.create_registry(packet.PacketType, bifrost.IPacket(packet.PacketData))
