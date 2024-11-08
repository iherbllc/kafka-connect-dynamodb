package com.trustpilot.connector.dynamodb.utils;

import com.amazonaws.util.Base64;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ByteBufferTypeAdapter extends TypeAdapter<ByteBuffer> {
    private static final Gson GSON = new Gson();

    @Override
    public void write(JsonWriter jsonWriter, ByteBuffer buffer) throws IOException {
        if (buffer == null)
        {
            jsonWriter.nullValue();
        }
        else {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            buffer.rewind();
            jsonWriter.value(Base64.encodeAsString(bytes));
        }
    }

    @Override
    public ByteBuffer read(JsonReader jsonReader) throws IOException {
        if (jsonReader.peek() == JsonToken.NULL)
        {
            jsonReader.nextNull();
            return null;
        }
        else if (jsonReader.peek() == JsonToken.BEGIN_OBJECT)
        {
            // A reasonable, limited effort for this use case to rehydrate a json formatted object representing
            // the internal properties of a ByteBuffer. This is for backwards compatibility with byte buffers
            // serialized using default gson serializers (i.e. ones that pluck out internal fields of types), which
            // are best avoided now because 1) it's an implementation detail of the jvm and subject to change
            // and 2) accessing these internal properties breaks on newer jvms enforcing stricktly
            // java modules security boundaries and it's best to avoid hacks to open modules up.
            Map properties = GSON.fromJson(jsonReader, Map.class);
            if (!properties.containsKey("hb"))
            {
                throw new RuntimeException("Unexpected ByteBuffer encoding. Does not contain a 'hb' field.");
            }

            List hb = (List)properties.get("hb");
            ByteBuffer buf = ByteBuffer.allocate(hb.size());

            for (Object elem : hb) {
                buf.put(((Double)elem).byteValue());
            }
            buf.position(((Double)properties.get("position")).intValue());

            return buf;
        }
        else if (jsonReader.peek() == JsonToken.STRING) {
            String base64 = jsonReader.nextString();
            byte[] bytes = Base64.decode(base64);
            return ByteBuffer.wrap(bytes);
        }
        throw new RuntimeException("Unexpected JSON token: " + jsonReader.peek().name());
    }
}
