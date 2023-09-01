package com.schleining.avro;

import org.apache.avro.Schema;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.MessageDecoder;
import org.apache.avro.specific.SpecificData;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for simple App.
 */
public class SerdeTest {

    public static final String TEST_KEY = "testKey";
    public static final String TEST_VALUE = "testValue";

    /**
     * When using a MessageDecoder for a class that was generated with "stringType" "string", then the decoder will
     * still put a Map&lt;Utf8,Utf8&gt; into the deserialized instance of the class.
     */
    @Test
    public void shouldDeserializeMapsIntoString() {
        RecordWithMapField record = RecordWithMapField.newBuilder().setMapField(Collections.singletonMap(TEST_KEY,
                TEST_VALUE)).build();
        byte[] serialized = serialize(record);
        RecordWithMapField deserialized = deserialize(serialized);

        assertEquals(TEST_VALUE, deserialized.getMapField().get(TEST_KEY));
    }

    private byte[] serialize(RecordWithMapField record) {
        try {
            return RecordWithMapField.getEncoder().encode(record).array();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RecordWithMapField deserialize(byte[] serialized) {
        MessageDecoder<RecordWithMapField> decoder;
        try {
            Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("avro/Schema.avsc"));
            decoder = new BinaryMessageDecoder<>(new SpecificData(), schema);
            return decoder.decode(serialized);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
