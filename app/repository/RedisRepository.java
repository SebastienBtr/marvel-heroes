package repository;

import com.fasterxml.jackson.databind.JsonNode;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import models.StatItem;
import models.TopStatItem;
import play.Logger;
import play.libs.Json;
import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");


    private final RedisClient redisClient;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection.async().zincrby("topVisited", 1, statItem.toJson().toString()).thenApply(res -> {
            connection.close();
            return !res.isNaN();
        });
    }

    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection.async().zadd("lastVisited", new Timestamp(new Date().getTime()).getTime(), statItem.toJson().toString()).thenApply(res -> {
            connection.close();
            return res;
        });
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection.async().zrevrange("lastVisited", 0, count-1).thenApply(lastVisited -> {
            connection.close();
            return lastVisited.stream().map(StatItem::fromJson).collect(Collectors.toList());
        });
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection.async().zrevrangeWithScores("topVisited", 0, count - 1).thenApply(topVisited -> {
            connection.close();
            return topVisited.stream().map(tv -> {
                return new TopStatItem(StatItem.fromJson(tv.getValue()), (long)tv.getScore());
            }).collect(Collectors.toList());
        });
    }
}
