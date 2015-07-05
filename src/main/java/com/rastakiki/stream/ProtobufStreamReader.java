package com.rastakiki.stream;

import com.google.protobuf.GeneratedMessage;
import com.rastakiki.protobufstream.protobuf.MessageParser;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ProtobufStreamReader<T extends GeneratedMessage> {

    private final InputStream inputStream;
    private final MessageParser<T> messageParser;
    private byte[] messageLengthByte;

    public ProtobufStreamReader(InputStream inputStream, MessageParser messageParser) {
        this.inputStream = inputStream;
        this.messageParser = messageParser;
    }

    public T readMessage() {
        try {
            return readNextMessage();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private T readNextMessage() throws IOException {
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

    public boolean hasNext() {
        try {
            messageLengthByte = new byte[Integer.BYTES];
            final int byteRead = IOUtils.read(inputStream, messageLengthByte);
            if (byteRead < Integer.BYTES) {
                return false;
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public T nextMessage() {
        if (!hasNext()) {
            return null;
        }
        return readMessage();
    }
}
