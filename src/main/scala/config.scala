package edu.mit.cryptdb

/** A set of flags that the optimizer uses to decide what to do */
case class OptimizerConfiguration(
  allowHomAggs: Boolean,
  sumFilterOpt: Boolean,
  groupByPushDown: Boolean
)

trait DefaultOptimizerConfiguration {
  val config: OptimizerConfiguration =
    OptimizerConfiguration(
      true, /* Allows use of hom aggregates */
      true, /* Allow sum(x) > const optimizations */
      true  /* Allow group by push-downs */
    )
}
