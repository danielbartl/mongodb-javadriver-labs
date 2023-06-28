package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

class DeleteOneDocumentTest {

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
    void testDeleteOne() {

        // given
        Bson filter = and(eq("student_id", 20000), eq("test", true));
        final long counted =
                grades.countDocuments(filter);

        final Document student = new Document("_id", new ObjectId());
        student.append("student_id", 20000)
                .append("scores", List.of(
                        new Document("type", "exam").append("score", Math.random() * 100),
                        new Document("type", "quiz").append("score", Math.random() * 100),
                        new Document("type", "homework").append("score", Math.random() * 100),
                        new Document("type", "homework").append("score", Math.random() * 100)
                ))
                .append("class_id", 666).
                append("test", true);

        grades.insertOne(student);

        // when
        final Bson filterDeleteOne =
                and(
                        eq("student_id", 20000),
                        eq("test", true));
        final DeleteResult deleteOneResult = grades.deleteOne(filterDeleteOne);

        // then
        Assertions.assertEquals(1, deleteOneResult.getDeletedCount());
        Assertions.assertEquals(counted, grades.countDocuments(filter));

    }

    @AfterEach
    void tearDown() {

        client.close();

    }
}