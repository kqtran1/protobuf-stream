package com.rastakiki.stream;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ProtobufStreamWriter<T extends GeneratedMessage> {

    private final OutputStream outputStream;

    public ProtobufStreamWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void writeMessage(T message) {
        try {
            outputStream.write(createMessageBytes(message));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] createMessageBytes(T message) throws InvalidProtocolBufferException {
        final byte[] tupleBytes = message.toByteArray();

        final int messageLength = tupleBytes.length;
        byte[] lengthBytes = ByteBuffer.allocate(Integer.BYTES).putInt(messageLength).array();

        byte[] entireMessage = new byte[tupleBytes.length + Integer.BYTES];
        System.arraycopy(lengthBytes, 0, entireMessage, 0, lengthBytes.length);
        System.arraycopy(tupleBytes, 0, entireMessage, lengthBytes.length, tupleBytes.length);
        return entireMessage;
    }

}
