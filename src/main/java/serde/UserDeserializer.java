package serde;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import myapp.User;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class UserDeserializer implements Closeable, AutoCloseable, Deserializer<User> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    static private Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public User deserialize(String topic, byte[] bytes) {
        try {
            // Transform the bytes to String
            String user = new String(bytes, CHARSET);
            // Return the User object created from the String 'user'
            JsonObject jsonObject = gson.fromJson(user, JsonElement.class).getAsJsonObject();
            return gson.fromJson(jsonObject.get("payload"), User.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    @Override
    public void close() {

    }
}