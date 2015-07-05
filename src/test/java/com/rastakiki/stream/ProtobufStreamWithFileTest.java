package com.rastakiki.stream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rastakiki.protobufstream.protobuf.MessageSteam.Tuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtobufStreamWithFileTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void should_read_messages_until_end_of_stream() throws IOException {
        File serializedMessage = createFileWith4Tuple();

        final ProtobufStreamReader<Tuple> reader = createStreamReader(new BufferedInputStream(new FileInputStream(serializedMessage)));

        final List<Tuple> tuples = new ArrayList<>();
        while (reader.hasNext()) {
            final Tuple tuple = reader.readMessage();
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
        return new ProtobufStreamReader<>(inputStream, bytes -> {
            try {
                return Tuple.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
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
