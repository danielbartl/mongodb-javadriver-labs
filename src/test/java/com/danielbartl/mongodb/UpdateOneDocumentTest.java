package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

class UpdateOneDocumentTest extends TestSupport {

    private MongoClient client;
    private MongoCollection<Document> grades;
    private Bson filter;

    @BeforeEach
    void setUp() {

        final String connectionString = System.getProperty("mongodb.uri");
        client = MongoClients.create(connectionString);
        final MongoDatabase database = client.getDatabase("sample_training");
        grades = database.getCollection("grades");

    }

    @Test
    void testUpdatingOneDocument() {

        // given
        final InsertOneResult result = grades.insertOne(generateNewGrade(20007d, 666d));
        Assertions.assertTrue(result.wasAcknowledged());

        // when
        filter = and(eq("student_id", 20007d), eq("test", true));
        final Bson update = Updates.set("comment", "You should learn MongoDB!");
        final UpdateResult updateResult = grades.updateOne(filter, update);
        Assertions.assertTrue(updateResult.wasAcknowledged());

        // then
        final Document found = grades.find(filter).first();
        Assertions.assertEquals("You should learn MongoDB!", found.get("comment"));

    }

    @AfterEach
    void tearDown() {

        grades.deleteOne(filter);

        client.close();

    }
}