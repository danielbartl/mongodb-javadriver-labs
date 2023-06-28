package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;

class FindOneAndUpdateTest {

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
    void testFindOneAndUpdate() {

        //given
        final Bson updates = Updates.combine(
                Updates.set("class_id", 666),
                Updates.push("scores", new Document().append("score", 45d).append("type", "homework")),
                Updates.push("comments", "Inserted if required for this test only."),
                Updates.set("test", true));
        grades.updateOne(
                Filters.eq("student_id", 20002), updates, new UpdateOptions().upsert(true));

        // when
        final Bson filterOneAndUpdate = eq("student_id", 20002);
        final Bson update1 = Updates.inc("x", 10);
        final Bson update2 = Updates.rename("class_id", "new_class_id");
        final Bson update3 = Updates.mul("scores.0.score", 2);
        final Bson update4 = Updates.addToSet("comments", "This comment is unique.");
        final Bson update5 = Updates.addToSet("comments", "This comment is unique.");
        final Bson allUpdates = Updates.combine(update1, update2, update3, update4, update5);
        final Document oldVersion = grades.findOneAndUpdate(filterOneAndUpdate, allUpdates);

        //then
        Assertions.assertEquals(20002, oldVersion.get("student_id"));
        Assertions.assertNull(oldVersion.get("new_class_id"));
        Assertions.assertEquals(666, oldVersion.get("class_id"));
        final List<Document> scores = oldVersion.getList("scores", Document.class);
        Assertions.assertEquals(45d, scores.get(0).get("score"));
        final List<String> comments = oldVersion.getList("comments", String.class);
        Assertions.assertEquals(1, comments.size());

    }

    @AfterEach
    void tearDown() {

        grades.deleteOne(Filters.eq("student_id", 20002));

        client.close();

    }
}