package hello_world;


import hello_world.model.pojo.Signal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class InputMessageDeserializationSchema implements DeserializationSchema<Signal> {
    @Override
    public Signal deserialize(byte[] message) throws IOException {
        return new Signal(new String(message));
    }

    @Override
    public boolean isEndOfStream(Signal nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Signal> getProducedType() {
        return TypeInformation.of(Signal.class);
    }
}