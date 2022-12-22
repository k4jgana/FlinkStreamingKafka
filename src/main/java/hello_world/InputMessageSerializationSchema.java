package hello_world;

import hello_world.model.pojo.Signal;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class InputMessageSerializationSchema implements SerializationSchema<String> {
    @Override
    public byte[] serialize(String s) {
        return s.getBytes();
    }
}
