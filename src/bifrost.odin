package bifrost

create_registry :: proc($T: typeid, $V: typeid) -> Registry(T, V) {
	return Registry(T, V){}
}

// registry := bifrost.create_registry(packet.PacketType, bifrost.IPacket(packet.PacketData))
