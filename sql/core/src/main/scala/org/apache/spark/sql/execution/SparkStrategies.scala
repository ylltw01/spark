/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{execution, AnalysisException, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.NormalizeFloatingNumbers
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.MemoryPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StructType

/**
 * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
 * with the query planner and is not designed to be stable across spark releases.  Developers
 * writing libraries should instead consider using the stable APIs provided in
 * [[org.apache.spark.sql.sources]]
 */
abstract class SparkStrategy extends GenericStrategy[SparkPlan] {
  // 根据传入的 LogicalPlan直接生成前述提到的 PlanLater节点
  override protected def planLater(plan: LogicalPlan): SparkPlan = PlanLater(plan)
}

case class PlanLater(plan: LogicalPlan) extends LeafExecNode {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}

abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SparkPlanner =>

  /**
   * 特殊 limit操作的执行计划   Plans special cases of limit operators.
   */
  object SpecialLimits extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ReturnAnswer(rootPlan) => rootPlan match {
        case Limit(IntegerLiteral(limit), Sort(order, true, child))
            if limit < conf.topKSortFallbackThreshold =>
          TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child)))
            if limit < conf.topKSortFallbackThreshold =>
          TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), child) =>
          CollectLimitExec(limit, planLater(child)) :: Nil
        case other => planLater(other) :: Nil
      }
      case Limit(IntegerLiteral(limit), Sort(order, true, child))
          if limit < conf.topKSortFallbackThreshold =>
        TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
      case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child)))
          if limit < conf.topKSortFallbackThreshold =>
        TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  /**
   * Select the proper physical plan for join based on join strategy hints, the availability of
   * equi-join keys and the sizes of joining relations. Below are the existing join strategies,
   * their characteristics and their limitations.
   *
   * - Broadcast hash join (BHJ):
   *     仅支持等值连接，join key 不需要有序。
   *     支持除 full outer join 外的所有 join 类型。
   *     当广播的表很小的时候，broadcast hash join 是比其他 join 算法更快。但是，广播这个操作是网络密集型的而且它还肯在某些情况下引起 OOM，尤其当广播的表比较大的时候。
   *     Only supported for equi-joins, while the join keys do not need to be sortable.
   *     Supported for all join types except full outer joins.
   *     BHJ usually performs faster than the other join algorithms when the broadcast side is
   *     small. However, broadcasting tables is a network-intensive operation and it could cause
   *     OOM or perform badly in some cases, especially when the build/broadcast side is big.
   *
   * - Shuffle hash join:
   *     仅支持等值连接，join key 不需要有序。
   *     支持除 full outer join 外的所有 join 类型。
   *     Only supported for equi-joins, while the join keys do not need to be sortable.
   *     Supported for all join types except full outer joins.
   *
   * - Shuffle sort merge join (SMJ):
   *     仅支持等值连接并且 join key 需要有序。
   *     支持所有 join 类型。
   *     Only supported for equi-joins and the join keys have to be sortable.
   *     Supported for all join types.
   *
   * - Broadcast nested loop join (BNLJ):
   *     支持等值连接和非等值连接。
   *     支持所有的 join 类型，但是实现如下：
   *         1. 在使用 right outer join 时广播左边表。
   *         2. 在使用 left outer, left semi, left anti join等时广播右边表。
   *         3. 在 inner-like join 时广播任意一边
   *     Supports both equi-joins and non-equi-joins.
   *     Supports all the join types, but the implementation is optimized for:
   *       1) broadcasting the left side in a right outer join;
   *       2) broadcasting the right side in a left outer, left semi, left anti or existence join;
   *       3) broadcasting either side in an inner-like join.
   *     For other cases, we need to scan the data multiple times, which can be rather slow.
   *
   * - Shuffle-and-replicate nested loop join (a.k.a. cartesian product join):
   *      支持等值连接和非等值连接。
   *      仅支持 inner like joins.
   *     Supports both equi-joins and non-equi-joins.
   *    join 操作相 关 的执行计划   Supports only inner like joins.
   */
  object JoinSelection extends Strategy with PredicateHelper {

    /**
     * 判断输入的 LogicalPlan，是否小于等于 spark.sql.autoBroadcastJoinThreshold = 10M 默认 10 兆
     * Matches a plan whose output should be small enough to be used in broadcast join.
     */
    private def canBroadcast(plan: LogicalPlan): Boolean = {
      plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
    }

    /**
     * Matches a plan whose single partition should be small enough to build a hash table.
     * 判断输入的 LogicalPlan，是否足够小以构建 hash 表，其大小要小于 spark.sql.autoBroadcastJoinThreshold = 10M *  spark.sql.shuffle.partitions = 200
     * 即是，其要满足构建广播的到 shuffler 个数的乘积
     * Note: this assume that the number of partition is fixed, requires additional work if it's
     * dynamic.
     */
    private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
    }

    /**
     * Returns whether plan a is much smaller (3X) than plan b.
     * 判断 LogicalPlan a * 3 是否比 LogicalPlan b 小，
     * 因为构建hash map 的消耗要远高于 排序，因此仅仅在其中一个表足够小于另一个表时候，才会去构建 hash map
     * The cost to build hash map is higher than sorting, we should only build hash map on a table
     * that is much smaller than other one. Since we does not have the statistic for number of rows,
     * use the size of bytes here as estimation.
     */
    private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
    }
    //  在使用到hashjoin的时候，比如（BroadcastHashJoinExec，ShuffledHashJoinExec）时候，
    //  能进行处理右表的join type 是：innerLike（inner，cross），left out，left semi，left anti
    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
      case _ => false
    }
    //  在使用到hashjoin的时候，比如（BroadcastHashJoinExec，ShuffledHashJoinExec）时候，
    //  能进行处理左表的join type 是：innerLike（inner，cross），right out
    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }

    private def getBuildSide(
        wantToBuildLeft: Boolean,
        wantToBuildRight: Boolean,
        left: LogicalPlan,
        right: LogicalPlan): Option[BuildSide] = {
      if (wantToBuildLeft && wantToBuildRight) {
        // returns the smaller side base on its estimated physical size, if we want to build the
        // both sides.
        // 如果join 左表和右表都可以build，则选择其中一个最小的
        Some(getSmallerSide(left, right))
      } else if (wantToBuildLeft) {
        Some(BuildLeft)
      } else if (wantToBuildRight) {
        Some(BuildRight)
      } else {
        None
      }
    }

    private def getSmallerSide(left: LogicalPlan, right: LogicalPlan) = {
      if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight else BuildLeft
    }

    private def hintToBroadcastLeft(hint: JoinHint): Boolean = {
      hint.leftHint.exists(_.strategy.contains(BROADCAST))
    }

    private def hintToBroadcastRight(hint: JoinHint): Boolean = {
      hint.rightHint.exists(_.strategy.contains(BROADCAST))
    }

    private def hintToShuffleHashLeft(hint: JoinHint): Boolean = {
      hint.leftHint.exists(_.strategy.contains(SHUFFLE_HASH))
    }

    private def hintToShuffleHashRight(hint: JoinHint): Boolean = {
      hint.rightHint.exists(_.strategy.contains(SHUFFLE_HASH))
    }

    private def hintToSortMergeJoin(hint: JoinHint): Boolean = {
      hint.leftHint.exists(_.strategy.contains(SHUFFLE_MERGE)) ||
        hint.rightHint.exists(_.strategy.contains(SHUFFLE_MERGE))
    }

    private def hintToShuffleReplicateNL(hint: JoinHint): Boolean = {
      hint.leftHint.exists(_.strategy.contains(SHUFFLE_REPLICATE_NL)) ||
        hint.rightHint.exists(_.strategy.contains(SHUFFLE_REPLICATE_NL))
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      // If it is an equi-join, we first look at the join hints w.r.t. the following order:
      // 如果是等值连接，会首先使用如下规则：
      //   1. broadcast：如果 join 类型支持则使用 broadcast hash join，如果 join 两边都能用于广播，则使用最小的那端进行广播。
      //   2. sort merge：如果 join key 需要有序则使用 sort merge join。
      //   3. shuffle hash：如果 join 类型支持则使用 shuffle hash join，如果 join 两端都能用于构建 hash 表，则选择最小的那端。
      //   4. shuffle replicate NL：如果 join 类型是 inner like，则使用笛卡尔积。

      //   1. broadcast hint: pick broadcast hash join if the join type is supported. If both sides
      //      have the broadcast hints, choose the smaller side (based on stats) to broadcast.
      //   2. sort merge hint: pick sort merge join if join keys are sortable.
      //   3. shuffle hash hint: We pick shuffle hash join if the join type is supported. If both
      //      sides have the shuffle hash hints, choose the smaller side (based on stats) as the
      //      build side.
      //   4. shuffle replicate NL hint: pick cartesian product if join type is inner like.
      //
      // If there is no hint or the hints are not applicable, we follow these rules one by one:
      // 如果未匹配合适的，则会使用如下规则：
      //   1. 如果其中一端比较小足以广播，并且 join 类型支持，则会使用 broadcast hash join。如果 join 两边都能用于广播，则使用最小的那端进行广播。
      //   2. 如果其中一端比较小足以构建本地 hash 表，并且其比其他端的表都更小，并且参数 spark.sql.join.preferSortMergeJoin = false。
      //   3. 如果 join 的 key 是有序的，则使用 sort merge join。
      //   4. 如果 join 类型是 inner like，则使用笛卡尔积。
      //   5. broadcast nested loop join 是最终的解决方案，可能会造成 OOM。

      //   1. Pick broadcast hash join if one side is small enough to broadcast, and the join type
      //      is supported. If both sides are small, choose the smaller side (based on stats)
      //      to broadcast.
      //   2. Pick shuffle hash join if one side is small enough to build local hash map, and is
      //      much smaller than the other side, and `spark.sql.join.preferSortMergeJoin` is false.
      //   3. Pick sort merge join if the join keys are sortable.
      //   4. Pick cartesian product if join type is inner like.
      //   5. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
      //      other choice.
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, hint) =>
        def createBroadcastHashJoin(buildLeft: Boolean, buildRight: Boolean) = {
          // 根据join type 判断左表是否可以广播，并且左表的hits 为 broadcast
          val wantToBuildLeft = canBuildLeft(joinType) && buildLeft
          // 根据join type 判断右表是否可以广播，并且右表的hits 为 broadcast
          val wantToBuildRight = canBuildRight(joinType) && buildRight
          getBuildSide(wantToBuildLeft, wantToBuildRight, left, right).map { buildSide =>
            Seq(joins.BroadcastHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              condition,
              planLater(left),
              planLater(right)))
          }
        }

        def createShuffleHashJoin(buildLeft: Boolean, buildRight: Boolean) = {
          // 根据join type 判断左表是否可以Hash表，并且左表的hits 为 shuffle_hash
          val wantToBuildLeft = canBuildLeft(joinType) && buildLeft
          // 根据join type 判断右表是否可以Hash表，并且右表的hits 为 shuffle_hash
          val wantToBuildRight = canBuildRight(joinType) && buildRight
          getBuildSide(wantToBuildLeft, wantToBuildRight, left, right).map { buildSide =>
            Seq(joins.ShuffledHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              condition,
              planLater(left),
              planLater(right)))
          }
        }
        // hits 为 shuffle_merge
        def createSortMergeJoin() = {
          // 左表字段是否能排序
          if (RowOrdering.isOrderable(leftKeys)) {
            Some(Seq(joins.SortMergeJoinExec(
              leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right))))
          } else {
            None
          }
        }

        def createCartesianProduct() = {
          // inner join 或者 cross join
          if (joinType.isInstanceOf[InnerLike]) {
            Some(Seq(joins.CartesianProductExec(planLater(left), planLater(right), condition)))
          } else {
            None
          }
        }

        def createJoinWithoutHint() = {
          createBroadcastHashJoin(canBroadcast(left), canBroadcast(right))
            .orElse {
              if (!conf.preferSortMergeJoin) {
                createShuffleHashJoin(
                  canBuildLocalHashMap(left) && muchSmaller(left, right),
                  canBuildLocalHashMap(right) && muchSmaller(right, left))
              } else {
                None
              }
            }
            .orElse(createSortMergeJoin())
            .orElse(createCartesianProduct())
            .getOrElse {
              // This join could be very slow or OOM
              val buildSide = getSmallerSide(left, right)
              // 最终解决方案使用 BroadcastNestedLoopJoinExec
              Seq(joins.BroadcastNestedLoopJoinExec(
                planLater(left), planLater(right), buildSide, joinType, condition))
            }
        }
// 优先 BroadcastHashJoin -> SortMergeJoin -> ShuffleHashJoin -> CartesianProduct -> BroadcastNestedLoopJoin
        createBroadcastHashJoin(hintToBroadcastLeft(hint), hintToBroadcastRight(hint))
          .orElse { if (hintToSortMergeJoin(hint)) createSortMergeJoin() else None }
          .orElse(createShuffleHashJoin(hintToShuffleHashLeft(hint), hintToShuffleHashRight(hint)))
          .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
          .getOrElse(createJoinWithoutHint())

      // 如果是非等值连接，将会首先根据如下规则：
      //   1. broadcast：选择 broadcast nested loop join，如果 join 两边都能用于广播，则使用最小的那端进行广播。
      //   2. shuffle replicate NL：如果 join 类型是 inner like，则使用笛卡尔积。
      // If it is not an equi-join, we first look at the join hints w.r.t. the following order:
      //   1. broadcast hint: pick broadcast nested loop join. If both sides have the broadcast
      //      hints, choose the smaller side (based on stats) to broadcast for inner and full joins,
      //      choose the left side for right join, and choose right side for left join.
      //   2. shuffle replicate NL hint: pick cartesian product if join type is inner like.
      //
      // If there is no hint or the hints are not applicable, we follow these rules one by one:
      //   1. 如果其中有一端足够小到可以广播，则使用 broadcast nested loop join。如果仅左表可以广播并且是左连接或者仅右表可以广播并且是右连接，将跳过。
      //      如果两边都比较小，则对于 inner 和 full join 广播更小的，对于右连接广播左表，对于左连接广播右表。
      //   2. 如果 join 类型是 inner like，则使用笛卡尔积。
      //   3. broadcast nested loop join 是最终的 join 方案，
      //   1. Pick broadcast nested loop join if one side is small enough to broadcast. If only left
      //      side is broadcast-able and it's left join, or only right side is broadcast-able and
      //      it's right join, we skip this rule. If both sides are small, broadcasts the smaller
      //      side for inner and full joins, broadcasts the left side for right join, and broadcasts
      //      right side for left join.
      //   2. Pick cartesian product if join type is inner like.
      //   3. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
      //      other choice. It broadcasts the smaller side for inner and full joins, broadcasts the
      //      left side for right join, and broadcasts right side for left join.
      case logical.Join(left, right, joinType, condition, hint) =>
        val desiredBuildSide = if (joinType.isInstanceOf[InnerLike] || joinType == FullOuter) {
          getSmallerSide(left, right)
        } else {
          // For perf reasons, `BroadcastNestedLoopJoinExec` prefers to broadcast left side if
          // it's a right join, and broadcast right side if it's a left join.
          // TODO: revisit it. If left side is much smaller than the right side, it may be better
          // to broadcast the left side even if it's a left join.
          if (canBuildLeft(joinType)) BuildLeft else BuildRight
        }

        def createBroadcastNLJoin(buildLeft: Boolean, buildRight: Boolean) = {
          val maybeBuildSide = if (buildLeft && buildRight) {
            Some(desiredBuildSide)
          } else if (buildLeft) {
            Some(BuildLeft)
          } else if (buildRight) {
            Some(BuildRight)
          } else {
            None
          }

          maybeBuildSide.map { buildSide =>
            Seq(joins.BroadcastNestedLoopJoinExec(
              planLater(left), planLater(right), buildSide, joinType, condition))
          }
        }

        def createCartesianProduct() = {
          if (joinType.isInstanceOf[InnerLike]) {
            Some(Seq(joins.CartesianProductExec(planLater(left), planLater(right), condition)))
          } else {
            None
          }
        }

        def createJoinWithoutHint() = {
          createBroadcastNLJoin(canBroadcast(left), canBroadcast(right))
            .orElse(createCartesianProduct())
            .getOrElse {
              // This join could be very slow or OOM
              Seq(joins.BroadcastNestedLoopJoinExec(
                planLater(left), planLater(right), desiredBuildSide, joinType, condition))
            }
        }

        createBroadcastNLJoin(hintToBroadcastLeft(hint), hintToBroadcastRight(hint))
          .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
          .getOrElse(createJoinWithoutHint())


      // --- Cases where this strategy does not apply ---------------------------------------------
      case _ => Nil
    }
  }

  /**
   * Used to plan streaming aggregation queries that are computed incrementally as part of a
   * [[StreamingQuery]]. Currently this rule is injected into the planner
   * on-demand, only when planning in a [[org.apache.spark.sql.execution.streaming.StreamExecution]]
   */
  object StatefulAggregationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case _ if !plan.isStreaming => Nil

      case EventTimeWatermark(columnName, delay, child) =>
        EventTimeWatermarkExec(columnName, delay, planLater(child)) :: Nil

      case PhysicalAggregation(
        namedGroupingExpressions, aggregateExpressions, rewrittenResultExpressions, child) =>

        if (aggregateExpressions.exists(PythonUDF.isGroupedAggPandasUDF)) {
          throw new AnalysisException(
            "Streaming aggregation doesn't support group aggregate pandas UDF")
        }

        val stateVersion = conf.getConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION)

        // Ideally this should be done in `NormalizeFloatingNumbers`, but we do it here because
        // `groupingExpressions` is not extracted during logical phase.
        val normalizedGroupingExpressions = namedGroupingExpressions.map { e =>
          NormalizeFloatingNumbers.normalize(e) match {
            case n: NamedExpression => n
            case other => Alias(other, e.name)(exprId = e.exprId)
          }
        }

        aggregate.AggUtils.planStreamingAggregation(
          normalizedGroupingExpressions,
          aggregateExpressions.map(expr => expr.asInstanceOf[AggregateExpression]),
          rewrittenResultExpressions,
          stateVersion,
          planLater(child))

      case _ => Nil
    }
  }

  /**
   * Used to plan the streaming deduplicate operator.
   */
  object StreamingDeduplicationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case Deduplicate(keys, child) if child.isStreaming =>
        StreamingDeduplicateExec(keys, planLater(child)) :: Nil

      case _ => Nil
    }
  }

  /**
   * Used to plan the streaming global limit operator for streams in append mode.
   * We need to check for either a direct Limit or a Limit wrapped in a ReturnAnswer operator,
   * following the example of the SpecialLimits Strategy above.
   * Streams with limit in Append mode use the stateful StreamingGlobalLimitExec.
   * Streams with limit in Complete mode use the stateless CollectLimitExec operator.
   * Limit is unsupported for streams in Update mode.
   */
  case class StreamingGlobalLimitStrategy(outputMode: OutputMode) extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ReturnAnswer(rootPlan) => rootPlan match {
        case Limit(IntegerLiteral(limit), child)
            if plan.isStreaming && outputMode == InternalOutputModes.Append =>
          StreamingGlobalLimitExec(limit, LocalLimitExec(limit, planLater(child))) :: Nil
        case _ => Nil
      }
      case Limit(IntegerLiteral(limit), child)
          if plan.isStreaming && outputMode == InternalOutputModes.Append =>
        StreamingGlobalLimitExec(limit, LocalLimitExec(limit, planLater(child))) :: Nil
      case _ => Nil
    }
  }

  object StreamingJoinStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, _)
          if left.isStreaming && right.isStreaming =>

          new StreamingSymmetricHashJoinExec(
            leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

        case Join(left, right, _, _, _) if left.isStreaming && right.isStreaming =>
          throw new AnalysisException(
            "Stream-stream join without equality predicate is not supported", plan = Some(plan))

        case _ => Nil
      }
    }
  }

  /**
   * 聚合算子相关的执行计划     Used to plan the aggregate operator for expressions based on the AggregateFunction2 interface.
   */
  object Aggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression]) =>
        val aggregateExpressions = aggExpressions.map(expr =>
          expr.asInstanceOf[AggregateExpression])

        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(_.aggregateFunction.children.toSet).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our `RewriteDistinctAggregates` should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
              "Spark user mailing list.")
        }

        // Ideally this should be done in `NormalizeFloatingNumbers`, but we do it here because
        // `groupingExpressions` is not extracted during logical phase.
        val normalizedGroupingExpressions = groupingExpressions.map { e =>
          NormalizeFloatingNumbers.normalize(e) match {
            case n: NamedExpression => n
            case other => Alias(other, e.name)(exprId = e.exprId)
          }
        }

        val aggregateOperator =
          if (functionsWithDistinct.isEmpty) {
            aggregate.AggUtils.planAggregateWithoutDistinct(
              normalizedGroupingExpressions,
              aggregateExpressions,
              resultExpressions,
              planLater(child))
          } else {
            aggregate.AggUtils.planAggregateWithOneDistinct(
              normalizedGroupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              planLater(child))
          }

        aggregateOperator

      case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if aggExpressions.forall(expr => expr.isInstanceOf[PythonUDF]) =>
        val udfExpressions = aggExpressions.map(expr => expr.asInstanceOf[PythonUDF])

        Seq(execution.python.AggregateInPandasExec(
          groupingExpressions,
          udfExpressions,
          resultExpressions,
          planLater(child)))

      case PhysicalAggregation(_, _, _, _) =>
        // If cannot match the two cases above, then it's an error
        throw new AnalysisException(
          "Cannot use a mixture of aggregate function and group aggregate pandas UDF")

      case _ => Nil
    }
  }

  object Window extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalWindow(
        WindowFunctionType.SQL, windowExprs, partitionSpec, orderSpec, child) =>
        execution.window.WindowExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case PhysicalWindow(
        WindowFunctionType.Python, windowExprs, partitionSpec, orderSpec, child) =>
        execution.python.WindowInPandasExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case _ => Nil
    }
  }

  protected lazy val singleRowRdd = sparkContext.parallelize(Seq(InternalRow()), 1)
  // 内存数据表扫描计划
  object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryTableScanExec(_, filters, mem)) :: Nil
      case _ => Nil
    }
  }

  /**
   * This strategy is just for explaining `Dataset/DataFrame` created by `spark.readStream`.
   * It won't affect the execution, because `StreamingRelation` will be replaced with
   * `StreamingExecutionRelation` in `StreamingQueryManager` and `StreamingExecutionRelation` will
   * be replaced with the real relation using the `Source` in `StreamExecution`.
   */
  object StreamingRelationStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case s: StreamingRelation =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case s: StreamingExecutionRelation =>
        StreamingRelationExec(s.toString, s.output) :: Nil
      case s: StreamingRelationV2 =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case _ => Nil
    }
  }

  /**
   * Strategy to convert [[FlatMapGroupsWithState]] logical operator to physical operator
   * in streaming plans. Conversion for batch plans is handled by [[BasicOperators]].
   */
  object FlatMapGroupsWithStateStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case FlatMapGroupsWithState(
        func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, stateEnc, outputMode, _,
        timeout, child) =>
        val stateVersion = conf.getConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION)
        val execPlan = FlatMapGroupsWithStateExec(
          func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, None, stateEnc, stateVersion,
          outputMode, timeout, batchTimestampMs = None, eventTimeWatermark = None, planLater(child))
        execPlan :: Nil
      case _ =>
        Nil
    }
  }

  /**
   * Strategy to convert EvalPython logical operator to physical operator.
   */
  object PythonEvals extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ArrowEvalPython(udfs, output, child) =>
        ArrowEvalPythonExec(udfs, output, planLater(child)) :: Nil
      case BatchEvalPython(udfs, output, child) =>
        BatchEvalPythonExec(udfs, output, planLater(child)) :: Nil
      case _ =>
        Nil
    }
  }
  // 对基本算子生成的执行计划 一对一的映射
  object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case d: DataWritingCommand => DataWritingCommandExec(d, planLater(d.query)) :: Nil
      case r: RunnableCommand => ExecutedCommandExec(r) :: Nil

      case MemoryPlan(sink, output) =>
        val encoder = RowEncoder(StructType.fromAttributes(output))
        LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy())) :: Nil

      case logical.Distinct(child) =>
        throw new IllegalStateException(
          "logical distinct operator should have been replaced by aggregate in the optimizer")
      case logical.Intersect(left, right, false) =>
        throw new IllegalStateException(
          "logical intersect  operator should have been replaced by semi-join in the optimizer")
      case logical.Intersect(left, right, true) =>
        throw new IllegalStateException(
          "logical intersect operator should have been replaced by union, aggregate" +
            " and generate operators in the optimizer")
      case logical.Except(left, right, false) =>
        throw new IllegalStateException(
          "logical except operator should have been replaced by anti-join in the optimizer")
      case logical.Except(left, right, true) =>
        throw new IllegalStateException(
          "logical except (all) operator should have been replaced by union, aggregate" +
            " and generate operators in the optimizer")
      case logical.ResolvedHint(child, hints) =>
        throw new IllegalStateException(
          "ResolvedHint operator should have been replaced by join hint in the optimizer")

      case logical.DeserializeToObject(deserializer, objAttr, child) =>
        execution.DeserializeToObjectExec(deserializer, objAttr, planLater(child)) :: Nil
      case logical.SerializeFromObject(serializer, child) =>
        execution.SerializeFromObjectExec(serializer, planLater(child)) :: Nil
      case logical.MapPartitions(f, objAttr, child) =>
        execution.MapPartitionsExec(f, objAttr, planLater(child)) :: Nil
      case logical.MapPartitionsInR(f, p, b, is, os, objAttr, child) =>
        execution.MapPartitionsExec(
          execution.r.MapPartitionsRWrapper(f, p, b, is, os), objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInR(f, p, b, is, os, key, value, grouping, data, objAttr, child) =>
        execution.FlatMapGroupsInRExec(f, p, b, is, os, key, value, grouping,
          data, objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInRWithArrow(f, p, b, is, ot, key, grouping, child) =>
        execution.FlatMapGroupsInRWithArrowExec(
          f, p, b, is, ot, key, grouping, planLater(child)) :: Nil
      case logical.MapPartitionsInRWithArrow(f, p, b, is, ot, child) =>
        execution.MapPartitionsInRWithArrowExec(
          f, p, b, is, ot, planLater(child)) :: Nil
      case logical.FlatMapGroupsInPandas(grouping, func, output, child) =>
        execution.python.FlatMapGroupsInPandasExec(grouping, func, output, planLater(child)) :: Nil
      case logical.MapElements(f, _, _, objAttr, child) =>
        execution.MapElementsExec(f, objAttr, planLater(child)) :: Nil
      case logical.AppendColumns(f, _, _, in, out, child) =>
        execution.AppendColumnsExec(f, in, out, planLater(child)) :: Nil
      case logical.AppendColumnsWithObject(f, childSer, newSer, child) =>
        execution.AppendColumnsWithObjectExec(f, childSer, newSer, planLater(child)) :: Nil
      case logical.MapGroups(f, key, value, grouping, data, objAttr, child) =>
        execution.MapGroupsExec(f, key, value, grouping, data, objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsWithState(
          f, key, value, grouping, data, output, _, _, _, timeout, child) =>
        execution.MapGroupsExec(
          f, key, value, grouping, data, output, timeout, planLater(child)) :: Nil
      case logical.CoGroup(f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr, left, right) =>
        execution.CoGroupExec(
          f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr,
          planLater(left), planLater(right)) :: Nil

      case logical.Repartition(numPartitions, shuffle, child) =>
        if (shuffle) {
          ShuffleExchangeExec(RoundRobinPartitioning(numPartitions), planLater(child)) :: Nil
        } else {
          execution.CoalesceExec(numPartitions, planLater(child)) :: Nil
        }
      case logical.Sort(sortExprs, global, child) =>
        execution.SortExec(sortExprs, global, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.ProjectExec(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.FilterExec(condition, planLater(child)) :: Nil
      case f: logical.TypedFilter =>
        execution.FilterExec(f.typedCondition(f.deserializer), planLater(f.child)) :: Nil
      case e @ logical.Expand(_, _, child) =>
        execution.ExpandExec(e.projections, e.output, planLater(child)) :: Nil
      case logical.Sample(lb, ub, withReplacement, seed, child) =>
        execution.SampleExec(lb, ub, withReplacement, seed, planLater(child)) :: Nil
      case logical.LocalRelation(output, data, _) =>
        LocalTableScanExec(output, data) :: Nil
      case logical.LocalLimit(IntegerLiteral(limit), child) =>
        execution.LocalLimitExec(limit, planLater(child)) :: Nil
      case logical.GlobalLimit(IntegerLiteral(limit), child) =>
        execution.GlobalLimitExec(limit, planLater(child)) :: Nil
      case logical.Union(unionChildren) =>
        execution.UnionExec(unionChildren.map(planLater)) :: Nil
      case g @ logical.Generate(generator, _, outer, _, _, child) =>
        execution.GenerateExec(
          generator, g.requiredChildOutput, outer,
          g.qualifiedGeneratorOutput, planLater(child)) :: Nil
      case _: logical.OneRowRelation =>
        execution.RDDScanExec(Nil, singleRowRdd, "OneRowRelation") :: Nil
      case r: logical.Range =>
        execution.RangeExec(r) :: Nil
      case r: logical.RepartitionByExpression =>
        exchange.ShuffleExchangeExec(r.partitioning, planLater(r.child)) :: Nil
      case ExternalRDD(outputObjAttr, rdd) => ExternalRDDScanExec(outputObjAttr, rdd) :: Nil
      case r: LogicalRDD =>
        RDDScanExec(r.output, r.rdd, "ExistingRDD", r.outputPartitioning, r.outputOrdering) :: Nil
      case _ => Nil
    }
  }
}
