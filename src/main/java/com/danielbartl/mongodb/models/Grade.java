package com.danielbartl.mongodb.models;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.types.ObjectId;

import java.util.List;

public record Grade(
        @BsonId
        ObjectId id,
        @BsonProperty("student_id")
        Double studentId,
        @BsonProperty("class_id")
        Double classId,
        boolean test,
        List<Score> scores) {

}
