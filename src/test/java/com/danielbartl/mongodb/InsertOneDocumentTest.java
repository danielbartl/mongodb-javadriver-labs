package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.InsertOneResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InsertOneDocumentTest extends TestSupport {

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
    void testInsertOneDocument() {

        // given
        filter = Filters.and(
                Filters.eq("student_id", 20005d),
                Filters.eq("test", true));
        final long counted = grades.countDocuments(filter);
        Assertions.assertEquals(0, counted);

        // when
        final InsertOneResult result = grades.insertOne(generateNewGrade(20005d, 1d));

        // then
        Assertions.assertNotNull(result.getInsertedId());
        final long countedAfter =
                grades.countDocuments(
                        Filters.and(
                                Filters.eq("student_id", 20005d),
                                Filters.eq("test", true)));
        Assertions.assertEquals(1, countedAfter);

    }

    @AfterEach
    void tearDown() {

        grades.deleteMany(filter);

        client.close();

    }

}