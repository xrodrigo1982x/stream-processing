package stream;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import stream.model.Tweet;
import stream.util.Topics;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;
import static stream.util.ProducerConsumerFactory.kafkaProducer;

public class TweetProducer {

    private KafkaProducer<String, Tweet> producer = kafkaProducer(Tweet.class, "tweet-producer");
    private Faker faker = new Faker();
    private List<String> users = range(0, 100).mapToObj(i -> faker.name().username()).collect(Collectors.toList());
    private static Long INTERVAL = 1000l;
    private static Random RANDOM = new Random(System.nanoTime());

    public static void main(String[] args) {
        TweetProducer tweetProducer = new TweetProducer();
        while (true) {
            tweetProducer.produce();
            try {
                Thread.sleep(INTERVAL + (INTERVAL * RANDOM.nextLong()));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void produce() {
        Collections.shuffle(users);
        Tweet tweet = Tweet.builder()
                .id(UUID.randomUUID().toString())
                .text(tweetText())
                .user(users.get(0))
                .date(new Date())
                .build();
        producer.send(new ProducerRecord<>(Topics.NEW_TWEET, tweet.getId(), tweet));
        System.out.println("Tweet produced: " + tweet);
    }

    private String tweetText() {
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
