package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ThreeMostPopulatesCitiesInTexasTest {

    private MongoClient client;
    private MongoCollection<Document> zips;

    @BeforeEach
    void setUp() {

        final String connectionString = System.getProperty("mongodb.uri");
        client = MongoClients.create(connectionString);
        final MongoDatabase database = client.getDatabase("sample_training");
        zips = database.getCollection("zips");

    }

    @Test
    @DisplayName("Using aggregation framework to retrieve three most populated cities in Texas.")
    void threeMostPopulatedCitiesInTexas() {

        // given the sample data already provided in MongoDB Atlas

        // when
        final Bson match = Aggregates.match(Filters.eq("state", "TX"));
        final Bson group = Aggregates.group("$city", Accumulators.sum("totalPop", "$pop"));
        final Bson project = Aggregates.project(
                Projections.fields(
                        Projections.excludeId(),
                        Projections.include("totalPop"),
                        Projections.computed("city", "$_id")
                ));
        final Bson sort = Aggregates.sort(Sorts.descending("totalPop"));
        final Bson limit = Aggregates.limit(3);
        final ArrayList<Document> result =
                zips.aggregate(Arrays.asList(match, group, project, sort, limit)).into(new ArrayList<>());

        // then
        result.forEach(c ->
                Assertions.assertTrue(List.of("HOUSTON", "DALLAS", "SAN ANTONIO").contains(c.get("city"))));

    }

    @AfterEach
    void tearDown() {

        client.close();

    }
}
