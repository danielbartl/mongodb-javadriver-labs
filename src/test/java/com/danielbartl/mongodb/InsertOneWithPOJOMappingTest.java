package com.danielbartl.mongodb;

import com.danielbartl.mongodb.models.Grade;
import com.danielbartl.mongodb.models.Score;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.singletonList;

class InsertOneWithPOJOMappingTest {

    private MongoClient client;
    private MongoCollection<Grade> grades;
    private Bson filter;

    @BeforeEach
    void setUp() {

        final String connectionString = System.getProperty("mongodb.uri");

        final CodecRegistry pojoCodecRegistry =
                CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build());
        final CodecRegistry codecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);
        final MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .codecRegistry(codecRegistry)
                .build();


        this.client = MongoClients.create(clientSettings);

        MongoDatabase database = client.getDatabase("sample_training");
        grades = database.getCollection("grades", Grade.class);

    }

    @Test
    void testMappingPOJO() {

        filter = and(
                eq("student_id", 20006d),
                eq("test", true)
        );

        // given
        final Grade existingGrade =
                grades.find(filter).first();
        Assertions.assertNull(existingGrade);

        // when
        final Score score = new Score("homework", 50d);
        final Grade grade =
                new Grade(new ObjectId(), 20006d, 10d, true, singletonList(score));
        final InsertOneResult result = grades.insertOne(grade);

        // then
        Assertions.assertNotNull(result.getInsertedId());
        final Grade foundGrade = grades.find(filter).first();
        Assertions.assertEquals(20006d, foundGrade.studentId());
        Assertions.assertEquals(10d, foundGrade.classId());
        Assertions.assertEquals(50d, foundGrade.scores().get(0).score());
        Assertions.assertEquals("homework", foundGrade.scores().get(0).type());
        Assertions.assertTrue(foundGrade.test());

    }

    @AfterEach
    void tearDown() {

        grades.deleteOne(filter);

        client.close();

    }
}