package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.mql.MqlArray;
import com.mongodb.client.model.mql.MqlBoolean;
import com.mongodb.client.model.mql.MqlDocument;
import com.mongodb.client.model.mql.MqlInteger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.mongodb.client.model.mql.MqlValues.current;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AggregationsArrayOperationsTest {

    private MongoClient client;
    private MongoCollection<Document> movies;

    @BeforeEach
    void setUp() {

        final String connectionString = System.getProperty("mongodb.uri");
        client = MongoClients.create(connectionString);
        final MongoDatabase database = client.getDatabase("aggregation");
        movies = database.getCollection("movies");

    }

    @Test
    void testArrayOperations() {

        // given
        movies.insertMany(
                List.of(
                        new Document("movie", "The Big Lebowski")
                                .append("showtimes",
                                        List.of(
                                                new Document("date", LocalDate.of(1999, 6, 1))
                                                        .append("ticketsBought", 100)
                                                        .append("seats", List.of(20, 80))
                                        )),
                        new Document("movie", "Top Gun")
                                .append("showtimes",
                                        List.of(
                                                new Document("date", LocalDate.of(1984, 5, 1))
                                                        .append("ticketsBought", 25)
                                                        .append("seats", List.of(10, 40))
                                        )),
                        new Document("movie", "Ghostbusters")
                                .append("showtimes",
                                        List.of(
                                                new Document("date", LocalDate.of(1988, 12, 25))
                                                        .append("ticketsBought", 140)
                                                        .append("seats", List.of(40, 180))
                                        )),
                        new Document("movie", "Back To The Future")
                                .append("showtimes",
                                        List.of(
                                                new Document("date", LocalDate.of(1986, 1, 1))
                                                        .append("ticketsBought", 240)
                                                        .append("seats", List.of(40, 200)),
                                                new Document("date", LocalDate.of(1988, 2, 1))
                                                        .append("ticketsBought", 119)
                                                        .append("seats", List.of(20, 100))
                                        ))
                ));

        // when
        final MqlArray<MqlDocument> showtimes = current().getArray("showtimes");

        Function<MqlDocument, MqlBoolean> predicate = showtime -> {

            final var seats = showtime.<MqlInteger>getArray("seats");
            final var totalSeats = seats.sum(n -> n);
            final var ticketsBought = showtime.getInteger("ticketsBought");
            return ticketsBought.lt(totalSeats);

        };

        final Bson project = Aggregates.project(
                Projections.fields(
                        Projections.excludeId(),
                        Projections.include("movie"),
                        Projections.computed("nonFullyBookedShows", showtimes.filter(predicate))));

        final List<Document> result = movies.aggregate(List.of(project)).into(new ArrayList<>());

        // then
        assertTrue(result.get(0).getList("nonFullyBookedShows", Document.class).isEmpty());
        assertFalse(result.get(1).getList("nonFullyBookedShows", Document.class).isEmpty());
        assertFalse(result.get(2).getList("nonFullyBookedShows", Document.class).isEmpty());
        assertFalse(result.get(3).getList("nonFullyBookedShows", Document.class).isEmpty());
        assertFalse(result.get(3).getList("nonFullyBookedShows", Document.class).isEmpty());

    }

    @AfterEach
    void tearDown() {

        movies.drop();

        client.close();

    }
}
