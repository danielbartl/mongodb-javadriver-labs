package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class FindOneAndDeleteTest {

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
    void testFindOneAndDelete() {

        // given
        final Document student = new Document("_id", new ObjectId());
        student.append("student_id", 20003)
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
        final Bson filter =
                Filters.and(
                        Filters.eq("student_id", 20003),
                        Filters.eq("test", true));
        final Document document = grades.findOneAndDelete(filter);

        // then
        Assertions.assertEquals(20003, document.get("student_id"));
        Assertions.assertEquals(666, document.get("class_id"));

    }

    @AfterEach
    void tearDown() {

        client.close();

    }
}