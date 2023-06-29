package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Projections.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AggregationsBasicTest {

    private MongoClient client;
    private MongoCollection<Document> restaurants;

    @BeforeEach
    void setUp() {

        final String connectionString = System.getProperty("mongodb.uri");
        client = MongoClients.create(connectionString);
        final MongoDatabase database = client.getDatabase("aggregation");
        restaurants = database.getCollection("restaurants");

    }

    @Test
    void testBasicAggregation() {

        // given
        restaurants.insertMany(
                Arrays.asList(
                        new Document("name", "Sun Bakery Trattoria").append("contact", new Document().append("phone", "386-555-0189").append("email", "SunBakeryTrattoria@example.org").append("location", Arrays.asList(-74.0056649, 40.7452371))).append("stars", 4).append("categories", Arrays.asList("Pizza", "Pasta", "Italian", "Coffee", "Sandwiches")),
                        new Document("name", "Blue Bagels Grill").append("contact", new Document().append("phone", "786-555-0102").append("email", "BlueBagelsGrill@example.com").append("location", Arrays.asList(-73.92506, 40.8275556))).append("stars", 3).append("categories", Arrays.asList("Bagels", "Cookies", "Sandwiches")),
                        new Document("name", "XYZ Bagels Restaurant").append("contact", new Document().append("phone", "435-555-0190").append("email", "XYZBagelsRestaurant@example.net").append("location", Arrays.asList(-74.0707363, 40.59321569999999))).append("stars", 4).append("categories", Arrays.asList("Bagels", "Sandwiches", "Coffee")),
                        new Document("name", "Hot Bakery Cafe").append("contact", new Document().append("phone", "264-555-0171").append("email", "HotBakeryCafe@example.net").append("location", Arrays.asList(-73.96485799999999, 40.761899))).append("stars", 4).append("categories", Arrays.asList("Bakery", "Cafe", "Coffee", "Dessert")),
                        new Document("name", "Green Feast Pizzeria").append("contact", new Document().append("phone", "840-555-0102").append("email", "GreenFeastPizzeria@example.com").append("location", Arrays.asList(-74.1220973, 40.6129407))).append("stars", 2).append("categories", Arrays.asList("Pizza", "Italian")),
                        new Document("name", "ZZZ Pasta Buffet").append("contact", new Document().append("phone", "769-555-0152").append("email", "ZZZPastaBuffet@example.com").append("location", Arrays.asList(-73.9446421, 40.7253944))).append("stars", 0).append("categories", Arrays.asList("Pasta", "Italian", "Buffet", "Cafeteria")),
                        new Document("name", "XYZ Coffee Bar").append("contact", new Document().append("phone", "644-555-0193").append("email", "XYZCoffeeBar@example.net").append("location", Arrays.asList(-74.0166091, 40.6284767))).append("stars", 5).append("categories", Arrays.asList("Coffee", "Cafe", "Bakery", "Chocolates")),
                        new Document("name", "456 Steak Restaurant").append("contact", new Document().append("phone", "990-555-0165").append("email", "456SteakRestaurant@example.com").append("location", Arrays.asList(-73.9365108, 40.8497077))).append("stars", 0).append("categories", Arrays.asList("Steak", "Seafood")),
                        new Document("name", "456 Cookies Shop").append("contact", new Document().append("phone", "604-555-0149").append("email", "456CookiesShop@example.org").append("location", Arrays.asList(-73.8850023, 40.7494272))).append("stars", 4).append("categories", Arrays.asList("Bakery", "Cookies", "Cake", "Coffee")),
                        new Document("name", "XYZ Steak Buffet").append("contact", new Document().append("phone", "229-555-0197").append("email", "XYZSteakBuffet@example.org").append("location", Arrays.asList(-73.9799932, 40.7660886))).append("stars", 3).append("categories", Arrays.asList("Steak", "Salad", "Chinese"))
                ));

        // when
        final Bson match = Aggregates.match(Filters.eq("categories", "Bakery"));
        final Bson group = Aggregates.group("$stars", Accumulators.sum("count", 1));
        final Bson rename = Aggregates.project(
                Projections.fields(
                        excludeId(),
                        computed("star_rating", "$_id"),
                        include("count")
                ));
        final Bson sort = Aggregates.sort(Sorts.descending("star_rating"));

        final List<Document> result =
                restaurants.aggregate(List.of(match, group, rename, sort)).into(new ArrayList<>());

        // then
        assertEquals(2, result.size());
        assertEquals(5, result.get(0).get("star_rating"));
        assertEquals(1, result.get(0).get("count"));
        assertEquals(4, result.get(1).get("star_rating"));
        assertEquals(2, result.get(1).get("count"));

    }

    @AfterEach
    void tearDown() {

        restaurants.deleteMany(Filters.empty());
        restaurants.drop();

        client.close();

    }
}
