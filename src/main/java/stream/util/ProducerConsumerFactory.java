package stream.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import stream.serialization.SerDes;

import java.util.Properties;

public class ProducerConsumerFactory extends KafkaClient {

    public static <T> KafkaProducer<String, T> kafkaProducer(Class<T> clazz, String groupId) {
        Properties props = kafkaProperties(groupId);
        return new KafkaProducer<>(props, Serdes.String().serializer(), new SerDes<>(clazz));
    }

    public static <T> KStream<String, T> kafkaStream(KStreamBuilder streamBuilder, Class<T> clazz, String topic){
        return streamBuilder.stream(Serdes.String(), new SerDes<>(clazz), topic);
    }

    public static void start(KStreamBuilder streamBuilder, String groupId){
        KafkaStreams streams = new KafkaStreams(streamBuilder, kafkaProperties(groupId));
        streams.start();
    }

}
