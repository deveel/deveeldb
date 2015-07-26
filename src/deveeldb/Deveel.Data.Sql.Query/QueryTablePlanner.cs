// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	class QueryTablePlanner {
		private List<TablePlan> tablePlans;

		public QueryTablePlanner() {
			tablePlans = new List<TablePlan>();
			HasJoin = false;
		}

		public bool HasJoin { get; private set; }

		public TablePlan SinglePlan {
			get {
				if (tablePlans.Count != 1)
					throw new InvalidOperationException("The planner has more than one table.");

				return tablePlans[0];
			}
		}

		private int IndexOfPlan(TablePlan plan) {
			int sz = tablePlans.Count;
			for (int i = 0; i < sz; ++i) {
				if (tablePlans[i] == plan)
					return i;
			}

			return -1;
		}

		private static TablePlan ConcatPlans(TablePlan left, TablePlan right, IQueryPlanNode plan) {
			// Merge the variable list
			var newVarList = new ObjectName[left.ColumnNames.Length + right.ColumnNames.Length];
			Array.Copy(left.ColumnNames, 0, newVarList, 0, left.ColumnNames.Length);
			Array.Copy(right.ColumnNames, 0, newVarList, left.ColumnNames.Length - 1, right.ColumnNames.Length);

			// Merge the unique table names list
			var newUniqueList = new string[left.UniqueNames.Length + right.UniqueNames.Length];
			Array.Copy(left.UniqueNames, 0, newUniqueList, 0, left.UniqueNames.Length);
			Array.Copy(right.UniqueNames, 0, newUniqueList, left.UniqueNames.Length - 1, right.UniqueNames.Length);

			// Return the new table source plan.
			return new TablePlan(plan, newVarList, newUniqueList);
		}

		private TablePlan MergePlans(TablePlan left, TablePlan right, IQueryPlanNode mergePlan) {
			// Remove the sources from the table list.
			tablePlans.Remove(left);
			tablePlans.Remove(right);

			// Add the concatenation of the left and right tables.
			var newPlan = ConcatPlans(left, right, mergePlan);
			newPlan.MergeJoin(left, right);
			newPlan.SetUpdated();

			AddPlan(newPlan);

			return newPlan;
		}

		private TablePlan FindPlan(ObjectName reference) {
			// If there is only 1 plan then assume the variable is in there.
			if (tablePlans.Count == 1)
				return tablePlans[0];

			foreach (var source in tablePlans) {
				if (source.ContainsColumn(reference))
					return source;
			}

			throw new ArgumentException("Unable to find table with variable reference: " + reference);
		}

		private TablePlan FindCommonPlan(IList<ObjectName> columnNames) {
			if (columnNames.Count == 0)
				return null;

			TablePlan pivotPlan = null;
			foreach (var columnName in columnNames) {
				var plan = FindPlan(columnName);
				if (pivotPlan == null) {
					pivotPlan = plan;
				} else if (plan != pivotPlan) {
					return null;
				}
			}

			return pivotPlan;
		}

		private void SetCachePoints() {
			foreach (var plan in tablePlans) {
				plan.SetCachePoint();
			}
		}

		private TablePlan JoinPlansForColumns(IEnumerable<ObjectName> columnNames) {
			// Collect all the plans that encapsulate these variables.
			var touchedPlans = new List<TablePlan>();

			foreach (var name in columnNames) {
				var plan = FindPlan(name);

				if (!touchedPlans.Contains(plan))
					touchedPlans.Add(plan);
			}

			return JoinToSingle(touchedPlans);
		}

		private TablePlan NaturalJoinAll() {
			int sz = tablePlans.Count;
			if (sz == 1)
				return tablePlans[0];

			// Produce a plan that naturally joins all tables.
			return JoinToSingle(tablePlans);
		}

		private static int AssertBeNaturalJoin(TablePlan plan1, TablePlan plan2) {
			if (plan1.LeftPlan == plan2 || plan1.RightPlan == plan2)
				return 0;
			if (plan1.LeftPlan != null && plan2.LeftPlan != null)
				// This is a left clash
				return 2;
			if (plan1.RightPlan != null && plan2.RightPlan != null)
				// This is a right clash
				return 1;
			if ((plan1.LeftPlan == null && plan2.RightPlan == null) ||
				(plan1.RightPlan == null && plan2.LeftPlan == null))
				// This means a merge between the plans is fine
				return 0;

			// Must be a left and right clash
			return 2;
		}

		private TablePlan NaturallyJoinPlans(TablePlan plan1, TablePlan plan2) {
			JoinType joinType;
			SqlExpression onExpr;
			TablePlan leftPlan, rightPlan;

			// Are the plans linked by common join information?
			if (plan1.RightPlan == plan2) {
				joinType = plan1.RightJoinType;
				onExpr = plan1.RightOnExpression;
				leftPlan = plan1;
				rightPlan = plan2;
			} else if (plan1.LeftPlan == plan2) {
				joinType = plan1.LeftJoinType;
				onExpr = plan1.LeftOnExpression;
				leftPlan = plan2;
				rightPlan = plan1;
			} else {
				// Assertion - make sure no join clashes!
				if ((plan1.LeftPlan != null && plan2.LeftPlan != null) ||
					(plan1.RightPlan != null && plan2.RightPlan != null)) {
					throw new InvalidOperationException("Plans can not be naturally join because " +
					                                    "the left/right join plans clash.");
				}

				// Else we must assume a non-dependent join (not an outer join).
				// Perform a natural join
				IQueryPlanNode node1 = new NaturalJoinNode(plan1.Plan, plan2.Plan);
				return MergePlans(plan1, plan2, node1);
			}

			// This means plan1 and plan2 are linked by a common join and ON
			// expression which we evaluate now.
			bool outerJoin;
			if (joinType == JoinType.Left) {
				// Mark the left plan
				leftPlan.UpdatePlan(new MarkerNode(leftPlan.Plan, "OUTER_JOIN"));
				outerJoin = true;
			} else if (joinType == JoinType.Right) {
				// Mark the right plan
				rightPlan.UpdatePlan(new MarkerNode(rightPlan.Plan, "OUTER_JOIN"));
				outerJoin = true;
			} else if (joinType == JoinType.Inner) {
				// Inner join with ON expression
				outerJoin = false;
			} else {
				throw new InvalidOperationException(String.Format("Join type ({0}) is not supported.", joinType));
			}

			// Make a Planner object for joining these plans.
			var planner = new QueryTablePlanner();
			planner.AddPlan(leftPlan.Clone());
			planner.AddPlan(rightPlan.Clone());

			// Evaluate the on expression
			var node = planner.LogicalEvaluate(onExpr);

			// If outer join add the left outer join node
			if (outerJoin)
				node = new LeftOuterJoinNode(node, "OUTER_JOIN");

			// And merge the plans in this set with the new node.
			return MergePlans(plan1, plan2, node);
		}

		private TablePlan JoinToSingle(IList<TablePlan> allPlans) {
			// If there are no plans then return null
			if (allPlans.Count == 0)
				return null;

			if (allPlans.Count == 1)
				// Return early if there is only 1 table.
				return allPlans[0];

			// Make a working copy of the plan list.
			var workingPlanList = new List<TablePlan>(allPlans);

			// We go through each plan in turn.
			while (workingPlanList.Count > 1) {
				var leftPlan = workingPlanList[0];
				var rightPlan = workingPlanList[1];

				// First we need to determine if the left and right plan can be
				// naturally joined.
				int status = AssertBeNaturalJoin(leftPlan, rightPlan);
				if (status == 0) {
					// Yes they can so join them
					var newPlan = NaturallyJoinPlans(leftPlan, rightPlan);

					// Remove the left and right plan from the list and add the new plan
					workingPlanList.Remove(leftPlan);
					workingPlanList.Remove(rightPlan);
					workingPlanList.Insert(0, newPlan);
				} else if (status == 1) {
					// No we can't because of a right join clash, so we join the left
					// plan right in hopes of resolving the clash.
					var newPlan = NaturallyJoinPlans(leftPlan, leftPlan.RightPlan);
					workingPlanList.Remove(leftPlan);
					workingPlanList.Remove(leftPlan.RightPlan);
					workingPlanList.Insert(0, newPlan);
				} else if (status == 2) {
					// No we can't because of a left join clash, so we join the left
					// plan left in hopes of resolving the clash.
					var newPlan = NaturallyJoinPlans(leftPlan, leftPlan.LeftPlan);
					workingPlanList.Remove(leftPlan);
					workingPlanList.Remove(leftPlan.LeftPlan);
					workingPlanList.Insert(0, newPlan);
				} else {
					throw new InvalidOperationException(String.Format("Natural join assessed status {0} is unknown.", status));
				}
			}

			// Return the working plan of the merged tables.
			return workingPlanList[0];
		}

		private IQueryPlanNode LogicalEvaluate(SqlExpression expression) {
			if (expression == null) {
				// Naturally join everything and return the plan.
				NaturalJoinAll();
				return SinglePlan.Plan;
			}

			// Plan the expression
			PlanExpression(expression);

			// Naturally join any straggling tables
			NaturalJoinAll();

			// Return the plan
			return SinglePlan.Plan;
		}

		private static void AddSingleColumnPlan(IList<SingleColumnPlan> list, TablePlan table, ObjectName columnName, ObjectName uniqueName, SqlExpression[] expParts, SqlExpressionType op) {
			var exp = SqlExpression.Binary(expParts[0], op, expParts[1]);

			// Is this source in the list already?
			foreach (var existingPlan in list) {
				if (existingPlan.TablePlan == table &&
					(columnName == null || existingPlan.ColumnName.Equals(columnName))) {
					// Append to end of current expression
					existingPlan.SetSource(columnName, SqlExpression.And(existingPlan.Expression, exp));
					return;
				}
			}

			// Didn't find so make a new entry in the list.
			list.Add(new SingleColumnPlan(table, columnName, uniqueName, exp));
		}

		private QueryTablePlanner Clone() {
			var copy = new QueryTablePlanner();

			int sz = tablePlans.Count;
			for (int i = 0; i < sz; ++i) {
				copy.tablePlans.Add(tablePlans[i].Clone());
			}

			// Copy the left and right links in the PlanTableSource
			for (int i = 0; i < sz; ++i) {
				var src = tablePlans[i];
				var mod = copy.tablePlans[i];

				// See how the left plan links to which index,
				if (src.LeftPlan != null) {
					int n = IndexOfPlan(src.LeftPlan);
					mod.LeftJoin(copy.tablePlans[n], src.LeftJoinType, src.LeftOnExpression);
				}

				// See how the right plan links to which index,
				if (src.RightPlan != null) {
					int n = IndexOfPlan(src.RightPlan);
					mod.RightJoin(copy.tablePlans[n], src.RightJoinType, src.RightOnExpression);
				}
			}

			return copy;
		}

		private void PlanAllOuterJoins() {
			int sz = tablePlans.Count;
			if (sz <= 1)
				return;

			// Make a working copy of the plan list.
			var workingPlanList = new List<TablePlan>(tablePlans);

			var plan1 = workingPlanList[0];
			for (int i = 1; i < sz; ++i) {
				var plan2 = workingPlanList[i];

				if (plan1.RightPlan == plan2) {
					plan1 = NaturallyJoinPlans(plan1, plan2);
				} else {
					plan1 = plan2;
				}
			}
		}

		private void PlanExpressionList(IEnumerable<SqlExpression> expressions) {
			var subLogicExpressions = new List<SqlBinaryExpression>();
			// The list of expressions that have a sub-select in them.
			var subQueryExpressions = new List<SqlBinaryExpression>();
			// The list of all constant expressions ( true = true )
			var constants = new List<SqlExpression>();
			// The list of pattern matching expressions (eg. 't LIKE 'a%')
			var patternExpressions = new List<SqlBinaryExpression>();
			// The list of all expressions that are a single variable on one
			// side, a conditional operator, and a constant on the other side.
			var singleVars = new List<SqlBinaryExpression>();
			// The list of multi variable expressions (possible joins)
			var multiVars = new List<SqlBinaryExpression>();

			foreach (var expression in expressions) {
				SqlBinaryExpression exp;

				if (!(expression is SqlBinaryExpression)) {
					// If this is not a binary expression we imply
					// [expression] = 'true'
					exp = SqlExpression.Equal(expression, SqlExpression.Constant(true));
				} else {
					exp = (SqlBinaryExpression) expression;
				}

				if (exp.ExpressionType.IsLogical()) {
					subLogicExpressions.Add(exp);
				} else if (exp.HasSubQuery()) {
					subQueryExpressions.Add(exp);
				} else if (exp.ExpressionType.IsPattern()) {
					patternExpressions.Add(exp);
				} else {
					// The list of variables in the expression.
					var columnNames = exp.DiscoverReferences().ToList();
					if (columnNames.Count == 0) {
						// These are ( 54 + 9 = 9 ), ( "z" > "a" ), ( 9.01 - 2 ), etc
						constants.Add(exp);
					} else if (columnNames.Count == 1) {
						// These are ( id = 90 ), ( 'a' < number ), etc
						singleVars.Add(exp);
					} else if (columnNames.Count > 1) {
						// These are ( id = part_id ),
						// ( cost_of + value_of < sold_at ), ( id = part_id - 10 )
						multiVars.Add(exp);
					} else {
						throw new InvalidOperationException("Invalid number of column names");
					}
				}
			}

			// The order in which expression are evaluated,
			// (ExpressionPlan)
			var evaluateOrder = new List<ExpressionPlan>();

			// Evaluate the constants.  These should always be evaluated first
			// because they always evaluate to either true or false or null.
			EvaluateConstants(constants, evaluateOrder);

			// Evaluate the singles.  If formed well these can be evaluated
			// using fast indices.  eg. (a > 9 - 3) is more optimal than
			// (a + 3 > 9).
			EvaluateSingles(singleVars, evaluateOrder);

			// Evaluate the pattern operators.  Note that some patterns can be
			// optimized better than others, but currently we keep this near the
			// middle of our evaluation sequence.
			EvaluatePatterns(patternExpressions, evaluateOrder);

			// Evaluate the sub-queries.  These are queries of the form,
			// (a IN ( SELECT ... )), (a = ( SELECT ... ) = ( SELECT ... )), etc.
			EvaluateSubQueries(subQueryExpressions, evaluateOrder);

			// Evaluate multiple variable expressions.  It's possible these are
			// joins.
			EvaluateMultiples(multiVars, evaluateOrder);

			// Lastly evaluate the sub-logic expressions.  These expressions are
			// OR type expressions.
			EvaluateSubLogic(subLogicExpressions, evaluateOrder);

			evaluateOrder.Sort();

			// And add each expression to the plan
			foreach (ExpressionPlan plan in evaluateOrder) {
				plan.AddToPlanTree();
			}
		}

		private void EvaluateSubLogic(List<SqlBinaryExpression> list, List<ExpressionPlan> plans) {
			foreach (var expression in list) {
				var orExprs = new[] {expression.Left, expression.Right};

				// An optimizations here;

				// If all the expressions we are ORing together are in the same table
				// then we should execute them before the joins, otherwise they
				// should go after the joins.

				// The reason for this is because if we can lesson the amount of work a
				// join has to do then we should.  The actual time it takes to perform
				// an OR search shouldn't change if it is before or after the joins.

				TablePlan common = null;

				foreach (var orExpr in orExprs) {
					var vars = orExpr.DiscoverReferences().ToArray();

					bool breakRule = false;

					// If there are no variables then don't bother with this expression
					if (vars.Any()) {
						// Find the common table source (if any)
						var ts = FindCommonPlan(vars);
						bool orAfterJoins = false;
						if (ts == null) {
							// No common table, so OR after the joins
							orAfterJoins = true;
						} else if (common == null) {
							common = ts;
						} else if (common != ts) {
							// No common table with the vars in this OR list so do this OR
							// after the joins.
							orAfterJoins = true;
						}

						if (orAfterJoins) {
							plans.Add(new SubLogicPlan(this, expression, 0.70f));

							// Continue to the next logic expression
							breakRule = true;
						}
					}

					if (!breakRule) {
						// Either we found a common table or there are no variables in the OR.
						// Either way we should evaluate this after the join.
						plans.Add(new SubLogicPlan(this, expression, 0.58f));
					}
				}
			}
		}

		private void EvaluateMultiples(List<SqlBinaryExpression> list, List<ExpressionPlan> plans) {
			// FUTURE OPTIMIZATION:
			//   This join order planner is a little primitive in design.  It orders
			//   optimizable joins first and least optimizable last, but does not
			//   take into account other factors that we could use to optimize
			//   joins in the future.

			foreach (var expression in list) {
				// Get the list of variables in the left hand and right hand side
				var lhsVar = expression.Left.AsReferenceName();
				var rhsVar = expression.Right.AsReferenceName();

				// Work out how optimizable the join is.
				// The calculation is as follows;
				// a) If both the lhs and rhs are a single variable then the
				//    optimizable value is set to 0.6f.
				// b) If only one of lhs or rhs is a single variable then the
				//    optimizable value is set to 0.64f.
				// c) Otherwise it is set to 0.68f (exhaustive select guarenteed).

				if (lhsVar == null && rhsVar == null) {
					// Neither lhs or rhs are single vars
					plans.Add(new ExhaustiveJoinPlan(this, expression));
				} else if (lhsVar != null && rhsVar != null) {
					// Both lhs and rhs are a single var (most optimizable type of
					// join).
					plans.Add(new StandardJoinPlan(this, expression, 0.60f));
				} else {
					// Either lhs or rhs is a single var
					plans.Add(new StandardJoinPlan(this, expression, 064f));
				}
			}
		}

		private void EvaluateSubQueries(List<SqlBinaryExpression> list, List<ExpressionPlan> plans) {
			foreach (var expression in list) {
				bool exhaustive;

				var op = expression.ExpressionType;
				if (op.IsSubQuery()) {
					// Must be an exhaustive sub-command
					exhaustive = true;
				} else {
					// Check that the left is a simple enough variable reference
					var leftColumn = expression.Left.AsReferenceName();
					if (leftColumn == null) {
						exhaustive = true;
					} else {
						// Check that the right is a sub-command plan.
						IQueryPlanNode rightPlan = expression.Right.AsQueryPlan();
						if (rightPlan == null)
							exhaustive = true;
						else {
							// Finally, check if the plan is correlated or not
							var cv = rightPlan.DiscoverQueryReferences(1);
							exhaustive = cv.Count != 0;
						}
					}
				}

				if (exhaustive) {
					// This expression could involve multiple variables, so we may need
					// to join.
					var columnNames = expression.DiscoverReferences().ToList();

					// Also find all correlated variables.
					int level = 0;
					var allCorrelated = expression.DiscoverQueryReferences(ref level);
					int sz = allCorrelated.Count;

					// If there are no variables (and no correlated variables) then this
					// must be a constant select, For example, 3 in ( select ... )
					if (!columnNames.Any() && sz == 0) {
						plans.Add(new ConstantPlan(this, expression));
					} else {
						columnNames.AddRange(allCorrelated.Select(cv => cv.Name));

						// An exhaustive expression plan which might require a join or a
						// slow correlated search.  This should be evaluated after the
						// multiple variables are processed.
						plans.Add(new ExhaustiveSubQueryPlan(this, columnNames.ToArray(), expression));
					}
				} else {
					plans.Add(new SimpleSubQueryPlan(this, expression));
				}
			}
		}

		private void EvaluatePatterns(List<SqlBinaryExpression> list, List<ExpressionPlan> plans) {
			foreach (var expression in list) {
				// If the LHS is a single column and the RHS is a constant then
				// the conditions are right for a simple pattern search.
				var leftColumnName = expression.Left.AsReferenceName();

				if (expression.IsConstant()) {
					plans.Add(new ConstantPlan(this, expression));
				} else if (leftColumnName != null &&
				           expression.Right.IsConstant()) {
					plans.Add(new SimplePatternPlan(this, leftColumnName, expression));
				} else {
					// Otherwise we must assume a complex pattern search which may
					// require a join.  For example, 'a + b LIKE 'a%'' or
					// 'a LIKE b'.  At the very least, this will be an exhaustive
					// search and at the worst it will be a join + exhaustive search.
					// So we should evaluate these at the end of the evaluation order.
					plans.Add(new ExhaustiveSelectPlan(this, expression));
				}
			}
		}

		private void EvaluateSingles(List<SqlBinaryExpression> list, List<ExpressionPlan> plans) {
			// The list of simple expression plans (lhs = single)
			var simplePlanList = new List<SingleColumnPlan>();
			// The list of complex function expression plans (lhs = expression)
			var complexPlanList = new List<SingleColumnPlan>();

			foreach (var expression in list) {
				// The single var
				ObjectName singleVar;
				SqlExpressionType op = expression.ExpressionType;
				SqlExpression left = expression.Left, right = expression.Right;

				if (op.IsSubQuery()) {
					singleVar = expression.Left.AsReferenceName();

					if (singleVar != null) {
						plans.Add(new SimpleSelectPlan(this, singleVar, op, expression.Right));
					} else {
						singleVar = expression.Left.DiscoverReferences().First();
						plans.Add(new ComplexSinglePlan(this, singleVar, expression));
					}
				} else {
					singleVar = expression.Left.DiscoverReferences().FirstOrDefault();
					if (singleVar == null) {
						// Reverse the expressions and the operator
						var tempExp = left;
						left = right;
						right = tempExp;
						op = op.Reverse();
						singleVar = left.DiscoverReferences().First();
					}

					var tableSource = FindPlan(singleVar);

					// Simple LHS?
					var v = left.AsReferenceName();
					if (v != null) {
						AddSingleColumnPlan(simplePlanList, tableSource, v, singleVar, new []{left, right}, op);
					} else {
						// No, complex lhs
						AddSingleColumnPlan(complexPlanList, tableSource, null, singleVar, new []{left, right}, op);
					}
				}
			}

			plans.AddRange(simplePlanList.Select(plan => new SimpleSinglePlan(this, plan.UniqueName, plan.Expression)).Cast<ExpressionPlan>());
			plans.AddRange(complexPlanList.Select(plan => new ComplexSinglePlan(this, plan.UniqueName, plan.Expression)).Cast<ExpressionPlan>());
		}

		private void EvaluateConstants(List<SqlExpression> list, List<ExpressionPlan> plans) {
			// For each constant variable
			plans.AddRange(list.Select(expr => new ConstantPlan(this, expr)).Cast<ExpressionPlan>());
		}

		private void PlanExpression(SqlExpression expression) {
			if (expression is SqlBinaryExpression &&
				expression.ExpressionType.IsLogical()) {
				var binary = (SqlBinaryExpression) expression;

				if (expression.ExpressionType == SqlExpressionType.Or) {
					// parsing an OR block
					// Split left and right of logical operator.
					var exps = new[]{binary.Left, binary.Right};

					// If we are an 'or' then evaluate left and right and union the
					// result.

					// Before we branch set cache points.
					SetCachePoints();

					// Make copies of the left and right planner
					var leftPlanner = Clone();
					var rightPlanner = Clone();

					// Plan the left and right side of the OR
					leftPlanner.PlanExpression(exps[0]);
					rightPlanner.PlanExpression(exps[1]);

					// Fix the left and right planner so that they represent the same
					// 'group'.
					// The current implementation naturally joins all sources if the
					// number of sources is different than the original size.
					int leftSz = leftPlanner.tablePlans.Count;
					int rightSz = rightPlanner.tablePlans.Count;
					if (leftSz != rightSz || leftPlanner.HasJoin || rightPlanner.HasJoin) {
						// Naturally join all in the left and right plan
						leftPlanner.NaturalJoinAll();
						rightPlanner.NaturalJoinAll();
					}

					// Union all table sources, but only if they have changed.
					var leftTableList = leftPlanner.tablePlans;
					var rightTableList = rightPlanner.tablePlans;
					int sz = leftTableList.Count;

					// First we must determine the plans that need to be joined in the
					// left and right plan.
					var leftJoinList = new List<TablePlan>();
					var rightJoinList = new List<TablePlan>();
					for (int i = 0; i < sz; ++i) {
						var leftPlan = leftTableList[i];
						var rightPlan = rightTableList[i];
						if (leftPlan.IsUpdated || rightPlan.IsUpdated) {
							leftJoinList.Add(leftPlan);
							rightJoinList.Add(rightPlan);
						}
					}

					// Make sure the plans are joined in the left and right planners
					leftPlanner.JoinToSingle(leftJoinList);
					rightPlanner.JoinToSingle(rightJoinList);

					// Since the planner lists may have changed we update them here.
					leftTableList = leftPlanner.tablePlans;
					rightTableList = rightPlanner.tablePlans;
					sz = leftTableList.Count;

					var newTableList = new List<TablePlan>(sz);

					for (int i = 0; i < sz; ++i) {
						var leftPlan = leftTableList[i];
						var rightPlan = rightTableList[i];

						TablePlan newPlan;

						// If left and right plan updated so we need to union them
						if (leftPlan.IsUpdated || rightPlan.IsUpdated) {
							// In many causes, the left and right branches will contain
							//   identical branches that would best be optimized out.

							// Take the left plan, add the logical union to it, and make it
							// the plan for this.
							var node = new LogicalUnionNode(leftPlan.Plan, rightPlan.Plan);

							// Update the plan in this table list
							leftPlan.UpdatePlan(node);

							newPlan = leftPlan;
						} else {
							// If the left and right plan didn't update, then use the
							// left plan (it doesn't matter if we use left or right because
							// they are the same).
							newPlan = leftPlan;
						}

						// Add the left plan to the new table list we are creating
						newTableList.Add(newPlan);

					}

					// Set the new table list
					tablePlans = newTableList;
				} else if (expression.ExpressionType == SqlExpressionType.And) {
					PlanExpressionList(new[]{binary.Left, binary.Right});
				} else {
					throw new InvalidOperationException();
				}
			} else {
				PlanExpressionList(new []{expression});
			}
		}

		public void AddPlan(TablePlan tablePlan) {
			tablePlans.Add(tablePlan);
			HasJoin = true;
		}

		public void AddPlan(IQueryPlanNode plan, IFromTableSource tableSource) {
			var columns = tableSource.ColumnNames;
			var uniqueNames = new[] {tableSource.UniqueName};
			AddPlan(new TablePlan(plan, columns, uniqueNames));
		}

		public void JoinAt(int betweenIndex, JoinType joinType, SqlExpression onExpression) {
			var planLeft = tablePlans[betweenIndex];
			var planRight = tablePlans[betweenIndex + 1];
			planLeft.RightJoin(planRight, joinType, onExpression);
			planRight.LeftJoin(planLeft, joinType, onExpression);
		}

		public IQueryPlanNode PlanSearchExpression(SqlExpression searchExpression) {
			// First perform all outer tables.
			PlanAllOuterJoins();

			return LogicalEvaluate(searchExpression);
		}

		#region SingleColumnPlan

		class SingleColumnPlan {
			public SingleColumnPlan(TablePlan tablePlan, ObjectName columnName, ObjectName uniqueName, SqlExpression expression) {
				TablePlan = tablePlan;
				ColumnName = columnName;
				UniqueName = uniqueName;
				Expression = expression;
			}

			public TablePlan TablePlan { get; private set; }

			public ObjectName ColumnName { get; private set; }

			public ObjectName UniqueName { get; private set; }

			public SqlExpression Expression { get; private set; }

			public void SetSource(ObjectName columnName, SqlExpression expression) {
				ColumnName = columnName;
				Expression = expression;
			}
		}

		#endregion

		#region ExpressionPlan

		abstract class ExpressionPlan : IExpressionPlan {
			protected ExpressionPlan(float optimizeFactor) {
				OptimizeFactor = optimizeFactor;
			}

			public float OptimizeFactor { get; private set; }

			public abstract void AddToPlanTree();

			public int CompareTo(IExpressionPlan other) {
				return OptimizeFactor.CompareTo(other.OptimizeFactor);
			}

			int IComparable.CompareTo(object obj) {
				var other = (ExpressionPlan) obj;
				return CompareTo(other);
			}
		}

		#endregion

		#region ConstantPlan

		class ConstantPlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly SqlExpression expression;

			public ConstantPlan(QueryTablePlanner planner, SqlExpression expression) 
				: base(0f) {
				this.planner = planner;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				foreach (var tablePlan in planner.tablePlans) {
					tablePlan.UpdatePlan(new ConstantSelectNode(tablePlan.Plan, expression));
				}
			}
		}

		#endregion

		#region SimpleSelectPlan

		class SimpleSelectPlan : ExpressionPlan {
			private readonly ObjectName columnName;
			private readonly SqlExpressionType op;
			private readonly SqlExpression expression;
			private readonly QueryTablePlanner planner;

			public SimpleSelectPlan(QueryTablePlanner planner, ObjectName columnName, SqlExpressionType op, SqlExpression expression)
				: base(0.2f){
				this.planner = planner;
				this.columnName = columnName;
				this.op = op;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				var tablePlan = planner.FindPlan(columnName);
				tablePlan.UpdatePlan(new SimpleSelectNode(tablePlan.Plan, columnName, op, expression));
			}
		}

		#endregion

		#region SimpleSinglePlan

		class SimpleSinglePlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly ObjectName columnName;
			private readonly SqlExpression expression;

			public SimpleSinglePlan(QueryTablePlanner planner, ObjectName columnName, SqlExpression expression)
				: base(0.2f){
				this.planner = planner;
				this.columnName = columnName;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				var tablePlan = planner.FindPlan(columnName);
				tablePlan.UpdatePlan(new RangeSelectNode(tablePlan.Plan, expression));
			}
		}

		#endregion

		#region ComplexSinglePlan

		class ComplexSinglePlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly ObjectName columnName;
			private readonly SqlExpression expression;

			public ComplexSinglePlan(QueryTablePlanner planner, ObjectName columnName, SqlExpression expression)
				: base(0.8f) {
				this.planner = planner;
				this.columnName = columnName;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				var tablePlan = planner.FindPlan(columnName);
				tablePlan.UpdatePlan(new ExhaustiveSelectNode(tablePlan.Plan, expression));
			}
		}

		#endregion

		#region SimplePatternPlan

		class SimplePatternPlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly ObjectName columnName;
			private readonly SqlExpression expression;

			public SimplePatternPlan(QueryTablePlanner planner, ObjectName columnName, SqlExpression expression)
				: base(0.25f) {
				this.planner = planner;
				this.columnName = columnName;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				var tablePlan = planner.FindPlan(columnName);
				tablePlan.UpdatePlan(new SimplePatternSelectNode(tablePlan.Plan, expression));
			}
		}

		#endregion

		#region ExhaustiveSelectPlan

		class ExhaustiveSelectPlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly SqlExpression expression;

			public ExhaustiveSelectPlan(QueryTablePlanner planner, SqlExpression expression)
				: base(0.82f) {
				this.planner = planner;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				var columnNames = expression.DiscoverReferences();
				var tablePlan = planner.JoinPlansForColumns(columnNames);
				tablePlan.UpdatePlan(new ExhaustiveSelectNode(tablePlan.Plan, expression));
			}
		}

		#endregion

		#region ExhaustiveSubQueryPlan

		class ExhaustiveSubQueryPlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly ObjectName[] columnNames;
			private readonly SqlExpression expression;

			public ExhaustiveSubQueryPlan(QueryTablePlanner planner, ObjectName[] columnNames, SqlExpression expression)
				: base(0.85f) {
				this.planner = planner;
				this.columnNames = columnNames;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				var tablePlan = planner.JoinPlansForColumns(columnNames);
				tablePlan.UpdatePlan(new ExhaustiveSelectNode(tablePlan.Plan, expression));
			}
		}

		#endregion

		#region SimpleSubQueryPlan

		class SimpleSubQueryPlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly SqlBinaryExpression expression;

			public SimpleSubQueryPlan(QueryTablePlanner planner, SqlBinaryExpression expression)
				: base(0.3f) {
				this.planner = planner;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				var op = expression.ExpressionType;
				var columnName = expression.Left.AsReferenceName();
				var queryPlan = expression.Right.AsQueryPlan();

				var tablePlan = planner.FindPlan(columnName);
				var leftPlan = tablePlan.Plan;

				tablePlan.UpdatePlan(new NonCorrelatedAnyAllNode(leftPlan, queryPlan, new []{columnName}, op));
			}
		}

		#endregion

		#region ExhaustiveJoinPlan

		class ExhaustiveJoinPlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly SqlExpression expression;

			public ExhaustiveJoinPlan(QueryTablePlanner planner, SqlExpression expression)
				: base(0.68f) {
				this.planner = planner;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				var columnNames = expression.DiscoverReferences();
				var tablePlan = planner.JoinPlansForColumns(columnNames);
				tablePlan.UpdatePlan(new ExhaustiveSelectNode(tablePlan.Plan, expression));
			}
		}

		#endregion

		#region StandardJoinPlan

		class StandardJoinPlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly SqlBinaryExpression expression;

			public StandardJoinPlan(QueryTablePlanner planner, SqlBinaryExpression expression, float optimizeFactor)
				: base(optimizeFactor) {
				this.planner = planner;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				var op = expression.ExpressionType;
				var lhsVar = expression.Left.AsReferenceName();
				var rhsVar = expression.Right.AsReferenceName();
				var lhsVars = expression.Left.DiscoverReferences();
				var rhsVars = expression.Right.DiscoverReferences();

				var lhsPlan = planner.JoinPlansForColumns(lhsVars);
				var rhsPlan = planner.JoinPlansForColumns(rhsVars);

				if (lhsPlan != rhsPlan) {
					// If either the LHS or the RHS is a single column then we can
					// optimize the join.

					if (lhsVar != null || rhsVar != null) {
						// If right column is a single and left column is not then we must
						// reverse the expression.
						JoinNode joinNode;
						if (lhsVar == null) {
							// Reverse the expressions and the operator
							joinNode = new JoinNode(rhsPlan.Plan, lhsPlan.Plan, rhsVar, op.Reverse(), expression.Left);
							planner.MergePlans(rhsPlan, lhsPlan, joinNode);
						} else {
							// Otherwise, use it as it is.
							joinNode = new JoinNode(lhsPlan.Plan, rhsPlan.Plan, lhsVar, op, expression.Right);
							planner.MergePlans(lhsPlan, rhsPlan, joinNode);
						}

						// Return because we are done
						return;
					}
				}

				// If we get here either both the lhs and rhs are complex expressions
				// or the lhs and rhs of the variable are not different plans, or
				// the operator is not a conditional.  Either way, we must evaluate
				// this via a natural join of the variables involved coupled with an
				// exhaustive select.  These types of queries are poor performing.

				var columnNames = expression.DiscoverReferences();
				var tablePlan = planner.JoinPlansForColumns(columnNames);
				tablePlan.UpdatePlan(new ExhaustiveSelectNode(tablePlan.Plan, expression));
			}
		}

		#endregion

		#region SubLogicPlan

		class SubLogicPlan : ExpressionPlan {
			private readonly QueryTablePlanner planner;
			private readonly SqlExpression expression;

			public SubLogicPlan(QueryTablePlanner planner, SqlExpression expression, float optimizeFactor)
				: base(optimizeFactor) {
				this.planner = planner;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				planner.PlanExpression(expression);
			}
		}

		#endregion
	}
}
