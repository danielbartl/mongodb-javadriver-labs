package com.danielbartl.mongodb;

import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public abstract class TestSupport {

    Document generateNewGrade(double studentId, double classId) {

        final Document student = new Document("_id", new ObjectId());
        student.append("student_id", studentId)
                .append("scores", List.of(
                        new Document("type", "exam").append("score", Math.random() * 100),
                        new Document("type", "quiz").append("score", Math.random() * 100),
                        new Document("type", "homework").append("score", Math.random() * 100),
                        new Document("type", "homework").append("score", Math.random() * 100)
                ))
                .append("class_id", classId)
                .append("test", true);

        return student;
    }

    List<Document> generateTenTestGradesFor(double studentId) {

        final ArrayList<Document> grades = new ArrayList<>();

        for (double classId = 1d; classId <= 10d; classId++) {

            final Document student = generateNewGrade(studentId, classId);
            grades.add(student);

        }

        return grades;
    }
}
