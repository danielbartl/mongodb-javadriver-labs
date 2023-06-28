package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.InsertManyResult;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

class InsertManyDocumentsTest extends TestSupport {

    private MongoClient client;
    private MongoCollection<Document> grades;

    @BeforeEach
    void setUp() {

        final String connectionString = System.getProperty("mongodb.uri");
        client = MongoClients.create(connectionString);
        final MongoDatabase database = client.getDatabase("sample_training");
        grades = database.getCollection("grades");

    }

    @Test
    void testInsertManyDocuments() {

        // given
        final long counted = grades.countDocuments(
                and(
                        eq("student_id", 20004d),
                        eq("test", true)
                )
        );
        Assertions.assertEquals(0, counted);

        // when
        final List<Document> list = generateTenTestGradesFor(20004d);
        final InsertManyResult result = grades.insertMany(list, new InsertManyOptions().ordered(false));

        // then
        Assertions.assertEquals(10, result.getInsertedIds().size());


    }

    @AfterEach
    void tearDown() {

        grades.deleteMany(
                and(
                        eq("student_id", 20004d),
                        eq("test", true)
                ));

        client.close();

    }
}