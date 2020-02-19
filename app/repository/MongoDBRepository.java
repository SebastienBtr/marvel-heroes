package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import play.libs.Json;
import utils.ReactiveStreamsUtils;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
        String query = "{ \"id\": \"" + heroId + "\" }";
        Document document = Document.parse(query);
        return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        String sort = "{ $sort:  {\"_id.yearAppearance\": -1}}";
        String group1 = "{ $group: { _id: {yearAppearance: \"$identity.yearAppearance\", universe: \"$identity.universe\"}, count: {$sum: 1}}}";
        String group2 = "{ $group: { _id: \"$_id\", byUniverse: { $push: { universe: \"$_id.universe\", count: \"$count\"}}}}";
        String match = "{ $match: { \"identity.yearAppearance\": { \"$ne\": \"\"}}}";
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(Document.parse(match));
        pipeline.add(Document.parse(group1));
        pipeline.add(Document.parse(group2));
        pipeline.add(Document.parse(sort));
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);

                                    })
                                    .collect(Collectors.toList());
                });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {
        String group = "{ $group: { _id: \"$powers\", count: {$sum: 1}}}";
        String unwind = "{ $unwind: \"$powers\"}";
        String sort = "{ $sort:  {count: -1}}";
        String limit = "{ $limit: "+ top + "}";
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(Document.parse(unwind));
        pipeline.add(Document.parse(group));
        pipeline.add(Document.parse(sort));
        pipeline.add(Document.parse(limit));
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                            .map(Document::toJson)
                            .map(Json::parse)
                            .map(jsonNode -> {
                                return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                            })
                            .collect(Collectors.toList());
                });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
        String query = "{ $group: { _id: \"$identity.universe\", count: {$sum: 1}}}";
        Document doc = Document.parse(query);
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(doc);
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                            .map(Document::toJson)
                            .map(Json::parse)
                            .map(jsonNode -> {
                                return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                            })
                            .collect(Collectors.toList());
                });
    }

}
