import avro
import json
import sys
import random
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from random import choice, uniform

def score(av, year):
    return av/10 * 0.8 + year/2020 *0.2

movies= [(294, "Die Hard"), (354, "Tree of Life"), (782, "A Walk in the Clouds"), (128, "The Big Lebowski"),
         (100, "Spiderman"), (120, "Pirates of The Caribbean"), (140, "La Grande Bellezza")]
schema = avro.schema.Parse(open("src/main/avro/updates.avsc", "rb").read())

random.seed(int(sys.argv[1]))
writer = DataFileWriter(open("movie-income.avro", "wb"), DatumWriter(), schema)
for i in range(200):
    movie = choice(movies)
    income = uniform(0,10)
    writer.append({"id": movie[0], "title": movie[1], "income": income})
writer.close()

reader = DataFileReader(open("movie-income.avro", "rb"), DatumReader())
updates = open("../updates-test.txt", "a")
for movie in reader:
    updates.write(json.dumps(movie)+"\n")
reader.close()
updates.close()