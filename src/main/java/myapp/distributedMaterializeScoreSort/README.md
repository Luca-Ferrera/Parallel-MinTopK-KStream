# Distributed materialize score and sort

## How to try it

### Local sorting
Run as many instance of `DistributedMaterializeScore` as you want.

Run `CentralizedSorting`  with `"dmss.env"` as argument.

output topic: `dmss-sorted-rated-movies`

### Distributed sorting, centralized aggregation and topK 
Run as many instance of `DistributedMaterializeScoreSort` as you want.

Run `CentralizedAggregatedSort`  with `"dmss.env"` as argument.

output topic: `dmss-topk-rated-movie`
