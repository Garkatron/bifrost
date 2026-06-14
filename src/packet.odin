package bifrost

import "core:mem"

MAX_BROADCAST_TARGETS :: 64

// PacketHeader is generic over the user-defined packet type enum T.
PacketHeader :: struct($T: typeid) {
	type:   T,
	length: u16, // payload size in bytes, filled by encode_packet
	seq:    u32, // injected by the network manager
	time:   i64, // injected by the network manager
}

// Packet pairs a header (keyed by enum T) with a payload union D.
Packet :: struct($T: typeid, $D: typeid) {
	header:  PacketHeader(T),
	payload: D,
}

make_packet :: proc($T: typeid, $D: typeid, type: T, payload: D) -> Packet(T, D) {
	return Packet(T, D){
		header  = PacketHeader(T){type = type},
		payload = payload,
	}
}

// IPacket holds the encode/decode pair for one concrete payload type,
// operating on the payload union D.
IPacket :: struct($D: typeid) {
	encode: proc(buf: ^[dynamic]byte, payload: D),
	decode: proc(buf: []byte, offset: ^int) -> D,
}


// generic_encode/generic_decode build IPacket entries for plain
// (pointer/slice-free) payload structs via raw memcopy.
//
// D         = the payload union type (e.g. packet.PacketData)
// ConcreteT = the concrete payload struct (e.g. packet.Ping), must be
//             a variant of D and must not contain pointers/slices.
generic_encode :: proc($D: typeid, $ConcreteT: typeid) -> proc(buf: ^[dynamic]byte, payload: D) {
	return proc(buf: ^[dynamic]byte, payload: D) {
		v := payload.(ConcreteT)
		bytes := mem.ptr_to_bytes(&v)
		append(buf, ..bytes)
	}
}

generic_decode :: proc($D: typeid, $ConcreteT: typeid) -> proc(buf: []byte, offset: ^int) -> D {
	return proc(buf: []byte, offset: ^int) -> D {
		v: ConcreteT
		mem.copy(&v, raw_data(buf[offset^:]), size_of(ConcreteT))
		offset^ += size_of(ConcreteT)
		return D(v)
	}
}

// encode_packet writes header + payload to a single byte buffer. The
// payload is encoded first so header.length can be filled with the
// real encoded size (needed for variable-size payloads).
encode_packet :: proc(reg: ^NetRegistry($T, $D), pkt: Packet(T, D)) -> []byte {
	payload_buf := make([dynamic]byte)
	defer delete(payload_buf)

	vt := reg.entries[pkt.header.type]
	vt.encode(&payload_buf, pkt.payload)

	h := pkt.header
	h.length = u16(len(payload_buf))

	buf := make([dynamic]byte, 0, size_of(PacketHeader(T)) + len(payload_buf))
	append(&buf, ..mem.ptr_to_bytes(&h))
	append(&buf, ..payload_buf[:])

	return buf[:]
}

// decode_packet reads payload_buf (already sized by header.length) into
// the payload union using the registry vtable for header.type.
decode_packet :: proc(reg: ^NetRegistry($T, $D), header: PacketHeader(T), payload_buf: []byte) -> (Packet(T, D), bool) {
	vt := reg.entries[header.type]
	if vt.decode == nil {
		return Packet(T, D){}, false
	}

	offset := 0
	payload := vt.decode(payload_buf, &offset)

	return Packet(T, D){header = header, payload = payload}, true
}


unwrap_message :: proc($T: typeid, $D: typeid, data: rawptr) -> (msg: ^NetworkMessage(T, D), payload: ^T, ok: bool) {
    if data == nil do return nil, nil, false
    msg = (^NetworkMessage(T, D))(data)
    payload, ok = &msg.packet.payload.(T)
    if !ok do return nil, nil, false
    return msg, payload, ok
}
