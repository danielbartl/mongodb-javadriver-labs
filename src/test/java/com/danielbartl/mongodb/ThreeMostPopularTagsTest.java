package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ThreeMostPopularTagsTest {

    private MongoClient client;
    private MongoCollection<Document> posts;

    @BeforeEach
    void setUp() {

        final String connectionString = System.getProperty("mongodb.uri");
        client = MongoClients.create(connectionString);
        final MongoDatabase database = client.getDatabase("sample_training");
        posts = database.getCollection("posts");


    }

    @Test
    @DisplayName("Using aggregation framework to retrieve three most popular tags.")
    void threeMostPopularTags() {

        // given the sample data already provided in MongoDB Atlas

        // when
        final Bson unwind = Aggregates.unwind("$tags");
        final Bson group = Aggregates.group(
                "$tags",
                Accumulators.sum("count", 1L),
                Accumulators.push("titles", "$title"));
        final Bson sort = Aggregates.sort(Sorts.descending("count"));
        final Bson limit = Aggregates.limit(3);
        final Bson project = Aggregates.project(
                Projections.fields(
                        Projections.excludeId(),
                        Projections.computed("tag", "$_id"),
                        Projections.include("count", "titles")));
        final List<Document> result = posts.aggregate(Arrays.asList(unwind, group, sort, limit, project)).into(new ArrayList<>());

        // then
        result.forEach(p -> Assertions.assertTrue(List.of("toad", "hair", "forest").contains(p.get("tag"))));

    }

    @AfterEach
    void tearDown() {

        client.close();

    }
}