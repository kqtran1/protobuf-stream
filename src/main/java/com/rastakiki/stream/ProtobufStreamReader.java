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
        final byte[] messageLengthByte = new byte[Integer.BYTES];
        IOUtils.read(inputStream, messageLengthByte);
        final int messageLength = bytesToInt(messageLengthByte);
        inputStream.mark(Long.BYTES);

        final byte[] messageContent = new byte[messageLength];
        IOUtils.read(inputStream, messageContent);
        return messageParser.parseMessage(messageContent);
    }

    public static int bytesToInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();
        return buffer.getInt();
    }


}
