package edu.mit.cryptdb

sealed trait HomType

case object HTNone extends HomType // homs disabled
case object HTRegular extends HomType // original cryptdb proposal

// the two below generate the same plans for now:
//   it is simple to make then generate different costs
//   it is not as simple to make them generate actually different plans
// neither option is currently implemented
case object HTRegWithColPacking extends HomType // original cryptdb proposal w/ row-level-packing
case object HTColumar extends HomType // columnar + (row/col) packing

/** A set of flags that the optimizer uses to decide what to do */
case class OptimizerConfiguration(
  homAggConfig: HomType,
  rowLevelPrecomputation: Boolean,
  sumFilterOpt: Boolean,
  greedyOnionSelection: Boolean,
  groupByPushDown: Boolean
)

trait DefaultOptimizerConfiguration {
  val config: OptimizerConfiguration =
    OptimizerConfiguration(
      HTColumar,  /* Allow use of columnar hom aggregates */
      true,  /* Allow row-level precomputation */
      true,  /* Allow sum(x) > const optimizations */
      false, /* Non-greedy, ie use cost-based model to select onion set */
      true   /* Allow group by push-downs */
    )
}
