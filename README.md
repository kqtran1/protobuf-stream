Stream protobuf message

Protobuf message streamin implementation

Protobuf is not meant to serialize huge graph of object.
The idea is to stream multiple messages with the following protocol:
| Protobuf Message Length | Protobuf Message | Protobuf Message Length | Protobuf Message | ...
