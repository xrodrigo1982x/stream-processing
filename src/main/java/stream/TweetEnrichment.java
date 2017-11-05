package stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import stream.biz.HashTagExtractor;
import stream.biz.QuoteExtractor;
import stream.model.Tweet;
import stream.serialization.SerDes;
import stream.util.ProducerConsumerFactory;
import stream.util.Topics;

import static stream.util.ProducerConsumerFactory.kafkaStream;

public class TweetEnrichment {

    private KStreamBuilder streamBuilder = new KStreamBuilder();
    private QuoteExtractor quoteExtractor = new QuoteExtractor();
    private HashTagExtractor hashTagExtractor = new HashTagExtractor();

    public static void main(String[] args) {
        new TweetEnrichment().run();
    }

    private void run() {
        KStream<String, Tweet> stream = kafkaStream(streamBuilder, Tweet.class, Topics.NEW_TWEET);
        stream.map((k, v) -> KeyValue.pair(k, quoteExtractor.apply(v)))
                .map((k, v) -> KeyValue.pair(k, hashTagExtractor.apply(v)))
                .to(Serdes.String(), new SerDes<>(Tweet.class), Topics.ENRICHED_TWEET);
        ProducerConsumerFactory.start(streamBuilder, "tweet-enrichment");
    }

}
