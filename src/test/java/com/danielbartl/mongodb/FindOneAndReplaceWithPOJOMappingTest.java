package com.danielbartl.mongodb;

import com.danielbartl.mongodb.models.Grade;
import com.danielbartl.mongodb.models.Score;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
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

import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.singletonList;

class FindOneAndReplaceWithPOJOMappingTest {

    private MongoClient client;
    private MongoCollection<Grade> grades;

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

        final MongoDatabase database = client.getDatabase("sample_training");
        grades = database.getCollection("grades", Grade.class);

    }

    @Test
    void testMappingPOJO() {

        // given
        final Score initialScore = new Score("homework", 50d);
        final Grade grade =
                new Grade(new ObjectId(), 20006d, 10d, true, singletonList(initialScore));
        final InsertOneResult result = grades.insertOne(grade);
        Assertions.assertNotNull(result.getInsertedId());

        // when
        Score newScore = new Score("exam", 42d);
        final Grade newGrade =
                new Grade(
                        result.getInsertedId().asObjectId().getValue(),
                        20006d,
                        11d
                        , true,
                        List.of(initialScore, newScore));

        final Bson filterByGradeId = Filters.eq("_id", result.getInsertedId());
        final FindOneAndReplaceOptions options = new FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER);
        final Grade updatedGrade = grades.findOneAndReplace(filterByGradeId, newGrade, options);
        Assertions.assertNotNull(updatedGrade);
        Assertions.assertEquals(20006d, updatedGrade.studentId());
        Assertions.assertEquals(11d, updatedGrade.classId());
        Assertions.assertEquals(2, updatedGrade.scores().size());

    }


    @AfterEach
    void tearDown() {

        grades.deleteOne(
                and(
                        eq("student_id", 20006d),
                        eq("test", true)
                ));

        client.close();

    }
}