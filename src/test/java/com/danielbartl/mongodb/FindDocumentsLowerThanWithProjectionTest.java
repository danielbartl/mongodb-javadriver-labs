package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;

class FindDocumentsLowerThanWithProjectionTest extends TestSupport {

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
        final List<Document> list = generateTenTestGradesFor(20001d);
        grades.insertMany(list);

        // when
        final ArrayList<Document> docs = grades
                .find(and(
                        eq("student_id", 20001d),
                        lte("class_id", 5),
                        eq("test", true)))
                .projection(fields(excludeId(), include("class_id", "student_id")))
                .sort(descending("class_id"))
                .skip(2)
                .limit(2)
                .into(new ArrayList<>());

        // then
        Assertions.assertEquals(2, docs.size());
        Assertions.assertEquals(3d, docs.get(0).get("class_id"));
        Assertions.assertEquals(20001d, docs.get(0).get("student_id"));
        Assertions.assertEquals(2d, docs.get(1).get("class_id"));
        Assertions.assertEquals(20001d, docs.get(1).get("student_id"));

    }

    @AfterEach
    void tearDown() {

        grades.deleteMany(and(eq("student_id", 20001d), eq("test", true)));

        client.close();

    }
}