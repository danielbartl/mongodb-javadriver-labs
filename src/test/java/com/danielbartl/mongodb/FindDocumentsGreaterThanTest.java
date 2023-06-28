package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;

class FindDocumentsGreaterThanTest {

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
    void testFindDocuments() {

        // given
        final Document student = new Document("_id", new ObjectId());
        student.append("student_id", 20001d)
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
        final ArrayList<Document> list =
                grades.find(and(gte("student_id", 20001d), eq("test", true))).
                        into(new ArrayList<>());

        // then
        Assertions.assertEquals(1, list.size());
        Assertions.assertEquals(20001d, list.get(0).get("student_id"));
        Assertions.assertEquals(666, list.get(0).get("class_id"));

    }

    @AfterEach
    void tearDown() {

        grades.deleteMany(and(eq("student_id", 20001d), eq("test", true)));

        client.close();

    }

}