package com.rastakiki.stream;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rastakiki.protobufstream.protobuf.MessageParser;
import com.rastakiki.protobufstream.protobuf.MessageSteam.Tuple;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore("ad-hoc performance test => Use JMH ?")
public class ProtobufStreamReaderPerfTest {

    public static final int MESSAGE_NUMBER = 100_000_000;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void should_read_messages_until_end_of_stream() throws IOException {
        File serializedMessage = createFileWith4Tuple();

        final ProtobufStreamReader<Tuple> reader = createStreamReader(new FileInputStream(serializedMessage));

        long messageCount = 0L;
        final long startTime = System.currentTimeMillis();
        for (Tuple tuple : reader) {
            messageCount++;
        }
        System.out.println("Time to read: " + (System.currentTimeMillis() - startTime) / 1000);
        assertThat(messageCount).isEqualTo(MESSAGE_NUMBER);
    }

    private File createFileWith4Tuple() throws IOException {
        File serializedMessage = temporaryFolder.newFile();
        serializedMessage.createNewFile();

        final OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(serializedMessage));
        final ProtobufStreamWriter<Tuple> writer = new ProtobufStreamWriter<>(outputStream);

        for (int i = 0; i < MESSAGE_NUMBER; i++) {
            writer.writeMessage(createTuple(i, "Unique Message " + i));
        }
        outputStream.close();
        System.out.println(serializedMessage.length() / 1024 / 1024 / 1024);
        return serializedMessage;
    }

    private ProtobufStreamReader<Tuple> createStreamReader(InputStream inputStream) {
        return new ProtobufStreamReader<>(inputStream, new MessageParser() {
            @Override
            public GeneratedMessage parseMessage(byte[] bytes) {
                try {
                    return Tuple.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private Tuple createTuple(long messageId, String messageValue) {
        return Tuple.newBuilder()
                .setId(messageId)
                .setValue(messageValue)
                .build();
    }

}
