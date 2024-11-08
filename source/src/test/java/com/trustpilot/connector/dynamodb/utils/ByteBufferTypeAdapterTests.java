package com.trustpilot.connector.dynamodb.utils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.Base64;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class ByteBufferTypeAdapterTests {
    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(ByteBuffer.class, new ByteBufferTypeAdapter())
            .create();

    @Test
    public void shouldSerializeByteBufferAttributeValue()
    {
        byte[] bytes = "sajdlfjaslkjflsajflkasjflkaj".getBytes(StandardCharsets.UTF_8);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        AttributeValue av = new AttributeValue();
        av.setB(byteBuffer);

        JsonElement jsonTree = gson.toJsonTree(av);
        byte[] fromJson = Base64.decode(jsonTree.getAsJsonObject().get("b").getAsString());
        assertArrayEquals(bytes, fromJson);
    }

    @Test
    public void shouldDeserializeByteBufferAttributeValue()
    {
        String json = "{\"b\": \"c2FqZGxmamFzbGtqZmxzYWpmbGthc2pmbGthag==\"}";
        AttributeValue av = gson.fromJson(json, AttributeValue.class);

        assertEquals("sajdlfjaslkjflsajflkasjflkaj", StandardCharsets.UTF_8.decode(av.getB()).toString());
    }

    @Test
    public void shouldDeserializeByteBufferAttributeValueFromRaw()
    {
        // Handle the case where we are loading a "legacy" json encoded ByteBuffer
        // that contains internal fields (
        String legacyByteBufferJson = "{\"b\":{\"hb\":[115,97,106,100,108,102,106,97,115,108,107,106,102,108,115,97,106,102,108,107,97,115,106,102,108,107,97,106],\"offset\":0,\"isReadOnly\":false,\"bigEndian\":true,\"nativeByteOrder\":false,\"mark\":-1,\"position\":0,\"limit\":28,\"capacity\":28,\"address\":0}}";
        AttributeValue av = gson.fromJson(legacyByteBufferJson, AttributeValue.class);

        assertEquals("sajdlfjaslkjflsajflkasjflkaj", StandardCharsets.UTF_8.decode(av.getB()).toString());
    }

    @Test
    public void shouldSerializeNullByteBufferAttributeValue()
    {
        AttributeValue av = new AttributeValue();

        JsonElement jsonTree = gson.toJsonTree(av);
        assertFalse(jsonTree.getAsJsonObject().has("b"));
    }

    @Test
    public void shouldDeserializeNullByteBufferAttributeValue()
    {
        String json = "{\"b\": null}";
        AttributeValue av = gson.fromJson(json, AttributeValue.class);
        assertNull(av.getB());
    }

    @Test
    public void shouldSerializeEmptyByteBufferAttributeValue()
    {
        AttributeValue av = new AttributeValue();
        av.setB(ByteBuffer.allocate(0));

        JsonElement jsonTree = gson.toJsonTree(av);
        byte[] fromJson = Base64.decode(jsonTree.getAsJsonObject().get("b").getAsString());
        assertEquals(0, fromJson.length);
    }

    @Test
    public void shouldDeserializeEmptyByteBufferAttributeValue()
    {
        String json = "{\"b\": \"\"}";
        AttributeValue av = gson.fromJson(json, AttributeValue.class);
        assertEquals(0, av.getB().capacity());
    }
}