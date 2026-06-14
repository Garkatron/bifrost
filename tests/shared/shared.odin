package test_shared

MAX_CHAT_LEN :: 255

PacketType :: enum {
	Message
}

PacketData :: union {
	Message
}

Message :: struct {
 	length:  int,
    content: [MAX_CHAT_LEN]byte,
}
