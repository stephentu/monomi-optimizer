package edu.mit.cryptdb

/** A set of flags that the optimizer uses to decide what to do */
case class OptimizerConfiguration(
  allowHomAggs: Boolean,
  rowLevelPrecomputation: Boolean,
  columnarHomAgg: Boolean,
  sumFilterOpt: Boolean,
  greedyOnionSelection: Boolean,
  groupByPushDown: Boolean
)

trait DefaultOptimizerConfiguration {
  val config: OptimizerConfiguration =
    OptimizerConfiguration(
      true,  /* Allow use of hom aggregates */
      true,  /* Allow row-level precomputation */
      true,  /* Allow column-store-ish hom aggs (with col/row level packing) */
      true,  /* Allow sum(x) > const optimizations */
      false, /* Non-greedy, ie use cost-based model to select onion set */
      true   /* Allow group by push-downs */
    )
}
