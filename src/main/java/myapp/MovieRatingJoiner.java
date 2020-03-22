package myapp;

import myapp.avro.Movie;
import myapp.avro.RatedMovie;
import myapp.avro.Rating;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class MovieRatingJoiner implements ValueJoiner<Rating, Movie, RatedMovie> {
    public RatedMovie apply (Rating rating, Movie movie){
        return RatedMovie.newBuilder()
                .setId(movie.getId())
                .setTitle(movie.getTitle())
                .setReleaseYear(movie.getReleaseYear())
                .setRating(rating.getRating())
                .build();
    }
}
