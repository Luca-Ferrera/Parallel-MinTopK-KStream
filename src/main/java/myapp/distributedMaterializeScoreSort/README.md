# Distributed materialize score and sort

## How to try it

### Distributed scoring, centralized sorting
Run as many instances of `DistributedMaterializeScore` as you want.

Run `CentralizedSorting`  with `"dmss.env"` as argument.

output topic: `dmss-sorted-rated-movies`

### Distributed sorting, centralized aggregation and topK 
Run as many instances of `DistributedMaterializeScoreSort` as you want.

Run `CentralizedAggregatedSort`  with `"dmss.env"` as argument.

output topic: `dmss-topk-rated-movie`

### Physical Window: Distributed sorting, centralized aggregation
Run 3 instances of `PhysicalWindowDistributedMSS`.

Run `PhysicalWindowCentralizedAggregatedSort`.

Run both files with `physicalWindowDisMSS.env` as argument.

output topic: `pdmss-final-sorted-rated-movies`
