package stream.biz;

import stream.model.DetailedTweet;
import stream.model.Tweet;

import java.util.function.Function;

public class DetailedTweetCreator implements Function<Tweet, DetailedTweet> {
    @Override
    public DetailedTweet apply(Tweet tweet) {
        return DetailedTweet.builder().source(tweet).build();
    }
}
