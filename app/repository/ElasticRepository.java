package repository;

import com.fasterxml.jackson.databind.JsonNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
         return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                 .post(Json.parse("{" +
                         "\"from\":" + (page-1) * size  + "," +
                         "\"size\":" + size + "," +
                         "\"query\": {" +
                            "\"query_string\": {" +
                                "\"query\":\"*" + input + "*\"," +
                                "\"fields\": [" +
                                    "\"name.keyword^5\", \"aliases.keyword^4\", \"secretIdentities.keyword^4\", \"description.keyword^3\", \"partners.keyword^2\"" +
                                "]" +
                            "}" +
                         "}" +
                     "}"))
                 .thenApply(response -> {
                     JsonNode resjson = response.asJson();
                     int total = resjson.get("hits").get("total").get("value").asInt();
                     JsonNode resHeroes = resjson.get("hits").get("hits");
                     List<SearchedHero> heroes = new ArrayList<>();
                     for(JsonNode hero: resHeroes){
                        heroes.add(new SearchedHero(
                                hero.get("_id").asText(),
                                hero.get("_source").get("imageUrl").asText(),
                                hero.get("_source").get("name").asText(),
                                hero.get("_source").get("universe").asText(),
                                hero.get("_source").get("gender").asText()
                        ));
                     }
                     int totalPage = (int) Math.ceil((double)total/size);
                     return new PaginatedResults<>(
                             total,
                             page,
                             totalPage,
                             heroes
                     );
                 });
    }
    // Version without elasticsearch suggest
//    public CompletionStage<List<SearchedHero>> suggest(String input) {
//        int size = 5;
//        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
//                .post(Json.parse("{" +
//                        "\"size\":" + size + "," +
//                        "\"query\": {" +
//                        "\"query_string\": {" +
//                        "\"query\":\"*" + input + "*\"," +
//                        "\"fields\": [" +
//                        "\"name.keyword^5\", \"aliases.keyword^4\", \"secretIdentities.keyword^4\"" +
//                        "]" +
//                        "}" +
//                        "}" +
//                        "}"))
//                .thenApply(response -> {
//                    System.out.println(response.asJson());
//                    JsonNode resjson = response.asJson();
//                    JsonNode resHeroes = resjson.get("hits").get("hits");
//                    List<SearchedHero> heroes = new ArrayList<>();
//                    for(JsonNode hero: resHeroes){
//                        heroes.add(new SearchedHero(
//                                hero.get("_id").asText(),
//                                hero.get("_source").get("imageUrl").asText(),
//                                hero.get("_source").get("name").asText(),
//                                hero.get("_source").get("universe").asText(),
//                                hero.get("_source").get("gender").asText()
//                        ));
//                    }
//                    return heroes;
//                });
//    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        int size = 5;
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{" +
                            "\"suggest\" : {" +
                                "\"suggestion\" : {" +
                                    "\"prefix\": \""+ input + "\"," +
                                    "\"completion\": {" +
                                        "\"field\": \"suggest\"," +
                                        "\"size\":  5" +
                                    "}" +
                                "}" +
                            "}"+
                        "}"))
                .thenApply(response -> {
                    JsonNode resjson = response.asJson();
                    JsonNode resHeroes = resjson.get("suggest").get("suggestion").get(0).get("options");
                    List<SearchedHero> heroes = new ArrayList<>();
                    for(JsonNode hero: resHeroes){
                        heroes.add(new SearchedHero(
                                hero.get("_id").asText(),
                                hero.get("_source").get("imageUrl").asText(),
                                hero.get("_source").get("name").asText(),
                                hero.get("_source").get("universe").asText(),
                                hero.get("_source").get("gender").asText()
                        ));
                    }
                    return heroes;
                });
    }
}
