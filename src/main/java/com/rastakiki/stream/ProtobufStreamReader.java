package com.rastakiki.stream;

import com.google.protobuf.GeneratedMessage;
import com.rastakiki.protobufstream.protobuf.MessageParser;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ProtobufStreamReader<T extends GeneratedMessage> implements Iterable<T>, Cloneable {

    private final InputStream inputStream;
    private final MessageParser<T> messageParser;
    private int lastMessageLength = 0;

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
        if (lastMessageLength == 0) {
            return null;
        }

        final byte[] messageContent = new byte[lastMessageLength];
        IOUtils.read(inputStream, messageContent);
        final T message = messageParser.parseMessage(messageContent);
        lastMessageLength = 0;
        return message;
    }

    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    protected void finalize() throws Throwable {
        close();
    }

    private class ProtobufStreamIterator implements Iterator<T> {

        public int bufferNext() {
            try {
                final byte[] lastLengthByte = new byte[Integer.BYTES];
                final int byteRead = IOUtils.read(inputStream, lastLengthByte);
                if (byteRead < Integer.BYTES) {
                    return 0;
                }
                lastMessageLength = bytesToInt(lastLengthByte);
                return lastMessageLength;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return lastMessageLength != 0 || bufferNext() != 0;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return readMessage();
        }

        private int bytesToInt(byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.put(bytes, 0, Integer.BYTES);
            buffer.flip();
            return buffer.getInt();
        }
    }

    @Override
    public Iterator<T> iterator() {
        return new ProtobufStreamIterator();
    }
}
