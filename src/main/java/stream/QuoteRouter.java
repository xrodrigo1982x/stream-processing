package stream;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import stream.model.DetailedTweet;
import stream.util.ProducerConsumerFactory;
import stream.util.Topics;

import static stream.util.ProducerConsumerFactory.kafkaStream;

public class QuoteRouter {

    private KStreamBuilder streamBuilder = new KStreamBuilder();

    public static void main(String[] args) {
        new QuoteRouter().run();
    }

    private void run() {
        KStream<String, DetailedTweet> stream = kafkaStream(streamBuilder, DetailedTweet.class, Topics.ENRICHED_TWEET);
        stream
                .foreach((k, v) -> System.out.println(v));
                //.to(Serdes.String(), new SerDes<>(QuotedTweet.class), Topics.QUOTED_TWEET)
        ;
        ProducerConsumerFactory.start(streamBuilder, "tweet-enrichment");
    }

}
