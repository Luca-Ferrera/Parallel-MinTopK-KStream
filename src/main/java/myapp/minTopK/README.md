# Centralized MinTopK algorithm
## How to try it

Run `CentralizedMinTopK` with `"minTopK.env" topK` as argument.
Where topK is the value of the topK parameter.

## Clean data structures

Run `CentralizedMinTopK` with `"minTopK.env" topK "clean"` as arguments.
Where topK is the value of the topK parameter.

## Generate input data

Run `RatingsDriver`, it generates records like this one:
```
{"id": 294, "title": "Die Hard", "release_year": 1988, "rating": 4.496183638158378, "score": 0.5565263742209872}
```

## Output
 
output topic: `mintopk-movies`

output format: 
```
key = windowID (Long)
value = MinTopKEntry
```
