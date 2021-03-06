package com.rastakiki.stream;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rastakiki.protobufstream.protobuf.MessageParser;
import com.rastakiki.protobufstream.protobuf.MessageSteam.Tuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.io.IOUtils.toBufferedInputStream;
import static org.assertj.core.api.Assertions.assertThat;

public class ProtobufStreamReaderWithFileTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void should_read_messages_until_end_of_stream() throws IOException {
        File serializedMessage = createFileWith4Tuple();

        final ProtobufStreamReader<Tuple> reader = createStreamReader(toBufferedInputStream(new FileInputStream(serializedMessage)));

        final List<Tuple> tuples = new ArrayList<>();
        for (Tuple tuple : reader) {
            tuples.add(tuple);
        }
        assertThat(tuples).hasSize(4);
    }

    private File createFileWith4Tuple() throws IOException {
        File serializedMessage = temporaryFolder.newFile();
        serializedMessage.createNewFile();

        final BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(serializedMessage));
        final ProtobufStreamWriter<Tuple> writer = new ProtobufStreamWriter<>(outputStream);

        for (int i = 0; i < 4; i++) {
            writer.writeMessage(createTuple(i, "Unique Message " + i));
        }
        outputStream.close();
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
