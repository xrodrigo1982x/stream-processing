package stream.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

public class SerDes<T> implements Deserializer<T>, Serializer<T> {

    private ObjectMapper om;
    private Class<T> clazz;

    public SerDes(Class<T> clazz) {
        this.clazz = clazz;
        om = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, T element) {
        try {
            return om.writeValueAsString(element).getBytes(Charset.forName("UTF-8"));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            return om.readValue(new String(bytes, Charset.forName("UTF-8")), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
