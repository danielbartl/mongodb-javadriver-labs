package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeleteManyDocumentsTest extends TestSupport {

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
    void testDeleteMany() {

        // given
        final long counted = grades.countDocuments(Filters.eq("student_id", 20000));
        grades.insertMany(generateTenTestGradesFor(20000));

        // when
        final Bson filterDeleteMany =
                Filters.and(
                        Filters.eq("student_id", 20000),
                        Filters.eq("test", true));
        final DeleteResult deleteManyResult = grades.deleteMany(filterDeleteMany);

        // then
        Assertions.assertEquals(10, deleteManyResult.getDeletedCount());
        Assertions.assertEquals(counted, grades.countDocuments(Filters.eq("student_id", 20000)));

    }

    @AfterEach
    void tearDown() {

        client.close();

    }
}