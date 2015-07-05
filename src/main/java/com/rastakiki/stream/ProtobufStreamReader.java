package com.rastakiki.stream;

import com.google.protobuf.GeneratedMessage;
import com.rastakiki.protobufstream.protobuf.MessageParser;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ProtobufStreamReader<T extends GeneratedMessage> {

    private final InputStream inputStream;
    private final MessageParser<T> messageParser;

    public ProtobufStreamReader(ByteArrayInputStream inputStream, MessageParser messageParser) {
        this.inputStream = inputStream;
        this.messageParser = messageParser;
    }

    public T readMessage() {
        try {
            return readMessage(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private T readMessage(InputStream inputStream) throws IOException {
        final byte[] messageLengthByte = new byte[Long.BYTES];
        IOUtils.read(inputStream, messageLengthByte);
        final long messageLength = bytesToLong(messageLengthByte);
        inputStream.mark(Long.BYTES);

        final byte[] messageContent = new byte[(int) messageLength];
        IOUtils.read(inputStream, messageContent);
        return messageParser.parseMessage(messageContent);
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();
        return buffer.getLong();
    }


}
