package com.rastakiki.stream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rastakiki.protobufstream.protobuf.MessageSteam.Tuple;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtobufStreamReaderTest {

    @Test
    public void given_one_message_messages_should_deserialize_one_messages() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ProtobufStreamWriter<Tuple> writer = new ProtobufStreamWriter<>(outputStream);
        writer.writeMessage(createTuple(10L, "Unique Message 1"));

        final ProtobufStreamReader<Tuple> reader = createStreamReader(outputStream);
        final Tuple tuple1 = reader.readMessage();


        assertThat(tuple1.getId()).isEqualTo(10L);
        assertThat(tuple1.getValue()).isEqualTo("Unique Message 1");
    }

    @Test
    public void given_two_message_messages_should_deserialize_two_messages() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ProtobufStreamWriter<Tuple> writer = new ProtobufStreamWriter<>(outputStream);
        writer.writeMessage(createTuple(10L, "Unique Message 1"));
        writer.writeMessage(createTuple(20L, "Unique Message 2"));

        final ProtobufStreamReader<Tuple> reader = createStreamReader(outputStream);
        final Tuple tuple1 = reader.readMessage();
        final Tuple tuple2 = reader.readMessage();


        assertThat(tuple1.getId()).isEqualTo(10L);
        assertThat(tuple1.getValue()).isEqualTo("Unique Message 1");

        assertThat(tuple2.getId()).isEqualTo(20L);
        assertThat(tuple2.getValue()).isEqualTo("Unique Message 2");
    }

    private ProtobufStreamReader<Tuple> createStreamReader(ByteArrayOutputStream outputStream) {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
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
