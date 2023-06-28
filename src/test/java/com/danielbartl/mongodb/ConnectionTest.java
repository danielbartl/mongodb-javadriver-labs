package com.danielbartl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ConnectionTest {

    @Test
    void testConnection() {

        final String connectionString = System.getProperty("mongodb.uri");

        try (MongoClient client = MongoClients.create(connectionString)) {

            final List<Document> databases = client.listDatabases().into(new ArrayList<>());
            assertFalse(databases.isEmpty());
            assertEquals(12, databases.size());

        }
        
    }
}