package com.danielbartl.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpdateManyDocumentsTest extends TestSupport {

    private MongoClient client;
    private MongoCollection<Document> grades;
    private Bson manyFilter;

    @BeforeEach
    void setUp() {

        final String connectionString = System.getProperty("mongodb.uri");
        client = MongoClients.create(connectionString);
        final MongoDatabase database = client.getDatabase("sample_training");
        grades = database.getCollection("grades");

    }

    @Test
    void testUpdateManyDocuments() {

        // given
        final InsertManyResult result = grades.insertMany(generateTenTestGradesFor(20008d));
        Assertions.assertEquals(10, result.getInsertedIds().size());

        // when
        manyFilter = and(eq("student_id", 20008d), eq("test", true));
        String comment = "You will learn a lot if you read the MongoDB blog.";
        final Bson manyUpdate = push("comments", comment);
        final UpdateResult updateManyResult = grades.updateMany(manyFilter, manyUpdate);
        Assertions.assertEquals(10, updateManyResult.getModifiedCount());

        // then
        final FindIterable<Document> documents = grades.find(manyFilter);
        documents.forEach(
                d ->
                        assertTrue(d.getList("comments", String.class)
                                .contains("You will learn a lot if you read the MongoDB blog.")));

    }

    @AfterEach
    void tearDown() {

        grades.deleteMany(manyFilter);

        client.close();

    }
}