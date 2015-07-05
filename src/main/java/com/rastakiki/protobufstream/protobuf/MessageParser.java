package com.rastakiki.protobufstream.protobuf;

import com.google.protobuf.GeneratedMessage;

public interface MessageParser<T extends GeneratedMessage> {

    T parseMessage(byte[] bytes);

}
