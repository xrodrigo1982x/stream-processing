package stream;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import stream.model.InputTweet;
import stream.serialization.SerDes;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.IntStream.range;

public class MessageProducer extends KafkaClient {

    private KafkaProducer<String, InputTweet> producer;
    private Faker faker;
    private List<String> users;

    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer();
        while (true) {
            messageProducer.produce();
        }
    }

    public MessageProducer() {
        Properties props = super.kafkaProperties("tweet-producer");
        producer = new KafkaProducer<>(props, Serdes.String().serializer(), new SerDes<>(InputTweet.class));
        faker = new Faker();
        users = range(0, 100).mapToObj(i -> faker.name().username()).collect(Collectors.toList());
    }

    private void produce() {
        Collections.shuffle(users);
        InputTweet tweet = InputTweet.builder()
                .id(UUID.randomUUID().toString())
                .text(tweetText())
                .user(users.get(0))
                .date(new Date())
                .build();
        producer.send(new ProducerRecord<>("new-tweets", tweet));
    }

    private String tweetText() {
        String quote = null;
        String hashTags = null;

        String sentence = faker.lorem().sentence(10, 10);

        if(faker.number().numberBetween(0, 9) == 0) {
            int index = sentence.indexOf(' ', faker.number().numberBetween(1, sentence.length()));
            sentence = sentence.substring(0, index) + " @"+users.get(1) + sentence.substring(index, sentence.length());
        }

        if(faker.number().numberBetween(0, 5) == 0)
            sentence = sentence + faker.lorem().sentence(faker.number().numberBetween(1, 3));

        return sentence;
    }

}
