package myapp;

import myapp.avro.AverageMovie;
import myapp.avro.Movie;
import myapp.avro.Rating;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class MovieAverageJoiner implements ValueJoiner<Movie, Double, AverageMovie> {
    public AverageMovie apply (Movie movie, Double average){
        return AverageMovie.newBuilder()
                .setId(movie.getId())
                .setTitle(movie.getTitle())
                .setReleaseYear(movie.getReleaseYear())
                .setAverage(average)
                .build();
    }
}
