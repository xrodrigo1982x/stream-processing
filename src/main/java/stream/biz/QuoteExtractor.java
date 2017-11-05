package stream.biz;

import stream.model.DetailedTweet;

import java.util.function.Function;

public class QuoteExtractor implements Function<DetailedTweet, DetailedTweet> {
    @Override
    public DetailedTweet apply(DetailedTweet enrichedTweet) {
        return enrichedTweet;
    }
}
