package stream.biz;

import stream.model.Tweet;

import java.util.function.Function;

public class HashTagExtractor implements Function<Tweet, Tweet> {
    @Override
    public Tweet apply(Tweet enrichedTweet) {
        return enrichedTweet;
    }
}
