package serde;

import com.google.gson.Gson;
import myapp.User;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class UserSerializer implements Closeable, AutoCloseable, Serializer<User> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    static private Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, User user) {
        // Transform the User object to String
        String line = gson.toJson(user);
        // Return the bytes from the String 'line'
        return line.getBytes(CHARSET);
    }

    @Override
    public void close() {

    }
}