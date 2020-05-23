import avro
import json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from random import choice, uniform

def score(av, year):
    return av/10 * 0.8 + year/2020 *0.2

movies= [(294, "Die Hard", 1988), (354, "Tree of Life", 2011), (782, "A Walk in the Clouds", 1995), (128, "The Big Lebowski", 1998),
            (100, "Spiderman", 2002), (120, "Pirates of The Caribbean", 2003), (140, "La Grande Bellezza", 2013)]
schema = avro.schema.Parse(open("src/main/avro/scored-movie.avsc", "rb").read())

writer = DataFileWriter(open("scored-movie.avro", "wb"), DatumWriter(), schema)
for i in range(500000):
    movie = choice(movies)
    rating = uniform(0,10)
    writer.append({"id": movie[0], "title": movie[1], "release_year": movie[2], "rating": rating, "score": score(rating, movie[2])})
writer.close()

reader = DataFileReader(open("scored-movie.avro", "rb"), DatumReader())
scored_movies = open("score-movies.txt", "a")
for movie in reader:
    scored_movies.write(json.dumps(movie)+"\n")
reader.close()
scored_movies.close()