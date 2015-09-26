# Protobuf message streaming implementation

Protobuf is not meant to serialize huge graph of objects.
The idea is to stream multiple messages with the following protocol:


| Protobuf Message Length | Protobuf Message | Protobuf Message Length | Protobuf Message | ...


See https://developers.google.com/protocol-buffers/docs/techniques?hl=en
