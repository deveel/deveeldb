// 
//  Copyright 2010  Deveel
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

using System;
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.Query {
	public static partial class Planner {
		/// <summary>
		/// A table set planner that maintains a list of table dependence lists and
		/// progressively constructs a plan tree from the bottom up.
		/// </summary>
		private sealed class QueryTableSetPlanner {

			/// <summary>
			/// The list of <see cref="PlanTableSource"/> objects for each source being planned.
			/// </summary>
			private List<PlanTableSource> tableList;

			/// <summary>
			/// If a join has occurred since the planner was constructed or copied then
			/// this is set to true.
			/// </summary>
			private bool hasJoinOccurred;


			public QueryTableSetPlanner() {
				tableList = new List<PlanTableSource>();
				hasJoinOccurred = false;
			}

			/// <summary>
			/// Returns the single <see cref="PlanTableSource"/> for this planner.
			/// </summary>
			internal PlanTableSource SingleTableSource {
				get {
					if (tableList.Count != 1)
						throw new Exception("Not a single table source.");
					return tableList[0];
				}
			}

			/// <summary>
			/// Returns true if a join has occurred ('table_list' has been modified).
			/// </summary>
			private bool HasJoinOccured {
				get { return hasJoinOccurred; }
			}

			/// <summary>
			/// Add a <see cref="PlanTableSource"/> to this planner.
			/// </summary>
			/// <param name="source"></param>
			private void AddPlanTableSource(PlanTableSource source) {
				tableList.Add(source);
				hasJoinOccurred = true;
			}

			/// <summary>
			/// Adds a new table source to the planner given a Plan that 'creates'
			/// the source.
			/// </summary>
			/// <param name="plan"></param>
			/// <param name="fromDef">The <see cref="IFromTableSource"/> that describes the source 
			/// created by the plan.</param>
			public void AddTableSource(IQueryPlanNode plan, IFromTableSource fromDef) {
				VariableName[] allCols = fromDef.AllColumns;
				string[] uniqueNames = new string[] { fromDef.UniqueName };
				AddPlanTableSource(new PlanTableSource(plan, allCols, uniqueNames));
			}

			/// <summary>
			/// Returns the index of the given <see cref="PlanTableSource"/> in the 
			/// table list.
			/// </summary>
			/// <param name="source"></param>
			/// <returns></returns>
			private int IndexOfPlanTableSource(PlanTableSource source) {
				int sz = tableList.Count;
				for (int i = 0; i < sz; ++i) {
					if (tableList[i] == source)
						return i;
				}
				return -1;
			}

			/// <summary>
			/// Links the last added table source to the previous added table source
			/// through this joining information.
			/// </summary>
			/// <param name="betweenIndex">Represents the point in between the table sources that 
			/// the join should be setup for.</param>
			/// <param name="joinType"></param>
			/// <param name="onExpression"></param>
			/// <remarks>
			/// For example, to set the join between TableSource 0 and 1, use 0 as the between index. 
			/// A between index of 3 represents the join between TableSource index 2 and 2.
			/// </remarks>
			public void SetJoinInfoBetweenSources(int betweenIndex, JoinType joinType, Expression onExpression) {
				PlanTableSource planLeft = tableList[betweenIndex];
				PlanTableSource planRight = tableList[betweenIndex + 1];
				planLeft.SetRightJoinInfo(planRight, joinType, onExpression);
				planRight.SetLeftJoinInfo(planLeft, joinType, onExpression);
			}

			/// <summary>
			/// Forms a new PlanTableSource that's the concatination of the given two 
			/// <see cref="PlanTableSource"/> objects.
			/// </summary>
			/// <param name="left"></param>
			/// <param name="right"></param>
			/// <param name="plan"></param>
			/// <returns></returns>
			private static PlanTableSource ConcatTableSources(PlanTableSource left, PlanTableSource right, IQueryPlanNode plan) {
				// Merge the variable list
				VariableName[] newVarList = new VariableName[left.VariableNames.Length + right.VariableNames.Length];
				int i = 0;
				for (int n = 0; n < left.VariableNames.Length; ++n) {
					newVarList[i] = left.VariableNames[n];
					++i;
				}
				for (int n = 0; n < right.VariableNames.Length; ++n) {
					newVarList[i] = right.VariableNames[n];
					++i;
				}

				// Merge the unique table names list
				string[] newUniqueList = new string[left.UniqueNames.Length + right.UniqueNames.Length];
				i = 0;
				for (int n = 0; n < left.UniqueNames.Length; ++n) {
					newUniqueList[i] = left.UniqueNames[n];
					++i;
				}
				for (int n = 0; n < right.UniqueNames.Length; ++n) {
					newUniqueList[i] = right.UniqueNames[n];
					++i;
				}

				// Return the new table source plan.
				return new PlanTableSource(plan, newVarList, newUniqueList);
			}

			/// <summary>
			/// Joins two tables when a plan is generated for joining the two tables.
			/// </summary>
			/// <param name="left"></param>
			/// <param name="right"></param>
			/// <param name="merge_plan"></param>
			/// <returns></returns>
			private PlanTableSource MergeTables(PlanTableSource left, PlanTableSource right, IQueryPlanNode merge_plan) {
				// Remove the sources from the table list.
				tableList.Remove(left);
				tableList.Remove(right);
				// Add the concatination of the left and right tables.
				PlanTableSource cPlan = ConcatTableSources(left, right, merge_plan);
				cPlan.SetJoinInfoMergedBetween(left, right);
				cPlan.SetUpdated();
				AddPlanTableSource(cPlan);
				// Return the name plan
				return cPlan;
			}

			/// <summary>
			/// Finds and returns the PlanTableSource in the list of tables that
			/// contains the given <see cref="VariableName"/> reference.
			/// </summary>
			/// <param name="reference"></param>
			/// <returns></returns>
			private PlanTableSource FindTableSource(VariableName reference) {
				// If there is only 1 plan then assume the variable is in there.
				if (tableList.Count == 1)
					return tableList[0];

				foreach (PlanTableSource source in tableList) {
					if (source.ContainsVariable(reference))
						return source;
				}

				throw new Exception("Unable to find table with variable reference: " + reference);
			}

			/// <summary>
			/// Finds a common <see cref="PlanTableSource"/> that contains the list of variables given.
			/// </summary>
			/// <param name="variables"></param>
			/// <remarks>
			/// If the list is 0 or there is no common source then null is returned.
			/// </remarks>
			/// <returns></returns>
			private PlanTableSource FindCommonTableSource(IList<VariableName> variables) {
				if (variables.Count == 0)
					return null;

				PlanTableSource plan = FindTableSource(variables[0]);
				int i = 1;
				int sz = variables.Count;
				while (i < sz) {
					PlanTableSource p2 = FindTableSource(variables[i]);
					if (plan != p2) {
						return null;
					}
					++i;
				}

				return plan;
			}

			/// <summary>
			/// Sets a <see cref="CachePointNode"/> with the given key on 
			/// all of the plan table sources in <see cref="tableList"/>.
			/// </summary>
			/// <remarks>
			/// Note that this does not change the <i>update</i> status of the table sources. 
			/// If there is currently a <see cref="CachePointNode"/> on any of the 
			/// sources then no update is made.
			/// </remarks>
			private void SetCachePoints() {
				foreach (PlanTableSource plan in tableList) {
					if (!(plan.Plan is CachePointNode))
						plan.Plan = new CachePointNode(plan.Plan);
				}
			}

			/// <summary>
			/// Creates a single <see cref="PlanTableSource"/> that encapsulates all 
			/// the given variables in a single table.
			/// </summary>
			/// <param name="allVars"></param>
			/// <remarks>
			/// If this means a table must be joined with another using the natural join 
			/// conditions then this happens here.
			/// <para>
			/// The intention of this function is to produce a plan that encapsulates
			/// all the variables needed to perform a specific evaluation.
			/// </para>
			/// <para>
			/// This has the potential to cause 'natural join' situations which are bad 
			/// performance.  It is a good idea to perform joins using other methods before 
			/// this is used.
			/// </para>
			/// <para>
			/// This will change the 'table_list' variable in this class if tables are joined.
			/// </para>
			/// </remarks>
			/// <returns></returns>
			private PlanTableSource JoinAllPlansWithVariables(IList<VariableName> allVars) {
				// Collect all the plans that encapsulate these variables.
				List<PlanTableSource> touchedPlans = new List<PlanTableSource>();
				foreach (VariableName v in allVars) {
					PlanTableSource plan = FindTableSource(v);
					if (!touchedPlans.Contains(plan))
						touchedPlans.Add(plan);
				}

				// Now 'touchedPlans' contains a list of PlanTableSource for each
				// plan to be joined.

				return JoinAllPlansToSingleSource(touchedPlans);
			}

			/// <summary>
			/// Returns true if it is possible to naturally join the two plans.
			/// </summary>
			/// <param name="plan1"></param>
			/// <param name="plan2"></param>
			/// <remarks>
			/// Two plans can be joined under the following sitations:
			/// <list type="number">
			///		<item>The left or right plan of the first source points 
			///		to the second source.</item>
			///		<item>Either one has no left plan and the other has no 
			///		right plan, or one has no right plan and the other has 
			///		no left plan.</item>
			/// </list>
			/// </remarks>
			/// <returns></returns>
			private static int CanPlansBeNaturallyJoined(PlanTableSource plan1, PlanTableSource plan2) {
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

			/// <summary>
			/// Given a list of <see cref="PlanTableSource"/> objects, this will produce 
			/// a plan that naturally joins all the tables together into a single plan.
			/// </summary>
			/// <param name="allPlans"></param>
			/// <remarks>
			/// The join algorithm used is determined by the information in the FROM clause. 
			/// An OUTER JOIN, for example, will join depending on the conditions provided 
			/// in the ON clause.  If no explicit join method is provided then a natural join 
			/// will be planned.
			/// <para>
			/// Care should be taken with this because this method can produce natural joins 
			/// which are often optimized out by more appropriate join expressions that can 
			/// be processed before this is called.
			/// </para>
			/// <para>
			/// This will change the <see cref="tableList"/> variable in this class if tables 
			/// are joined.
			/// </para>
			/// </remarks>
			/// <returns>
			/// Returns null if no plans are provided.
			/// </returns>
			private PlanTableSource JoinAllPlansToSingleSource(IList<PlanTableSource> allPlans) {
				// If there are no plans then return null
				if (allPlans.Count == 0)
					return null;

				if (allPlans.Count == 1)
					// Return early if there is only 1 table.
					return allPlans[0];

				// Make a working copy of the plan list.
				List<PlanTableSource> workingPlanList = new List<PlanTableSource>(allPlans);

				// We go through each plan in turn.
				while (workingPlanList.Count > 1) {
					PlanTableSource leftPlan = workingPlanList[0];
					PlanTableSource rightPlan = workingPlanList[1];

					// First we need to determine if the left and right plan can be
					// naturally joined.
					int status = CanPlansBeNaturallyJoined(leftPlan, rightPlan);
					if (status == 0) {
						// Yes they can so join them
						PlanTableSource newPlan = NaturallyJoinPlans(leftPlan, rightPlan);
						// Remove the left and right plan from the list and add the new plan
						workingPlanList.Remove(leftPlan);
						workingPlanList.Remove(rightPlan);
						workingPlanList.Insert(0, newPlan);
					} else if (status == 1) {
						// No we can't because of a right join clash, so we join the left
						// plan right in hopes of resolving the clash.
						PlanTableSource newPlan = NaturallyJoinPlans(leftPlan, leftPlan.RightPlan);
						workingPlanList.Remove(leftPlan);
						workingPlanList.Remove(leftPlan.RightPlan);
						workingPlanList.Insert(0, newPlan);
					} else if (status == 2) {
						// No we can't because of a left join clash, so we join the left
						// plan left in hopes of resolving the clash.
						PlanTableSource newPlan = NaturallyJoinPlans(leftPlan, leftPlan.LeftPlan);
						workingPlanList.Remove(leftPlan);
						workingPlanList.Remove(leftPlan.LeftPlan);
						workingPlanList.Insert(0, newPlan);
					} else {
						throw new Exception("Unknown status: " + status);
					}
				}

				// Return the working plan of the merged tables.
				return workingPlanList[0];

			}

			/// <summary>
			/// Naturally joins two <see cref="PlanTableSource"/> objects in this planner.
			/// </summary>
			/// <param name="plan1"></param>
			/// <param name="plan2"></param>
			/// <remarks>
			/// When this method returns the actual plans will be joined together. This method 
			/// modifies <see cref="tableList"/>.
			/// </remarks>
			/// <returns></returns>
			private PlanTableSource NaturallyJoinPlans(PlanTableSource plan1, PlanTableSource plan2) {
				JoinType joinType;
				Expression onExpr;
				PlanTableSource leftPlan, rightPlan;
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
						throw new Exception(
						   "Assertion failed - plans can not be naturally join because " +
						   "the left/right join plans clash.");
					}

					// Else we must assume a non-dependant join (not an outer join).
					// Perform a natural join
					IQueryPlanNode node1 = new NaturalJoinNode(plan1.Plan, plan2.Plan);
					return MergeTables(plan1, plan2, node1);
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
					throw new Exception("Join type (" + joinType + ") is not supported.");
				}

				// Make a Planner object for joining these plans.
				QueryTableSetPlanner planner = new QueryTableSetPlanner();
				planner.AddPlanTableSource(leftPlan.Copy());
				planner.AddPlanTableSource(rightPlan.Copy());

				//      planner.printDebugInfo();

				// Evaluate the on expression
				IQueryPlanNode node = planner.LogicalEvaluate(onExpr);
				// If outer join add the left outer join node
				if (outerJoin)
					node = new LeftOuterJoinNode(node, "OUTER_JOIN");

				// And merge the plans in this set with the new node.
				return MergeTables(plan1, plan2, node);
			}

			/// <summary>
			/// Plans all outer joins.
			/// </summary>
			/// <remarks>
			/// This will change the <see cref="tableList"/> variable in this class if 
			/// tables are joined.
			/// </remarks>
			private void PlanAllOuterJoins() {
				int sz = tableList.Count;
				if (sz <= 1)
					return;

				// Make a working copy of the plan list.
				List<PlanTableSource> workingPlanList = new List<PlanTableSource>(tableList);

				PlanTableSource plan1 = workingPlanList[0];
				for (int i = 1; i < sz; ++i) {
					PlanTableSource plan2 = workingPlanList[i];

					if (plan1.RightPlan == plan2) {
						plan1 = NaturallyJoinPlans(plan1, plan2);
					} else {
						plan1 = plan2;
					}
				}
			}

			/// <summary>
			/// Naturally joins all remaining tables sources to make a final single
			/// plan which is returned.
			/// </summary>
			/// <remarks>
			/// This will change the <see cref="tableList"/> variable in this class if 
			/// tables are joined.
			/// </remarks>
			/// <returns></returns>
			private PlanTableSource NaturalJoinAll() {
				int sz = tableList.Count;
				if (sz == 1)
					return tableList[0];

				// Produce a plan that naturally joins all tables.
				return JoinAllPlansToSingleSource(tableList);
			}

			/// <summary>
			/// Adds a single var plan to the given list.
			/// </summary>
			/// <param name="list"></param>
			/// <param name="table"></param>
			/// <param name="variable"></param>
			/// <param name="singleVar"></param>
			/// <param name="expParts"></param>
			/// <param name="op"></param>
			private static void AddSingleVarPlanTo(IList<SingleVarPlan> list, PlanTableSource table, VariableName variable, VariableName singleVar, Expression[] expParts, Operator op) {
				Expression exp = new Expression(expParts[0], op, expParts[1]);
				// Is this source in the list already?
				foreach (SingleVarPlan plan1 in list) {
					if (plan1.TableSource == table &&
						(variable == null || plan1.Variable.Equals(variable))) {
						// Append to end of current expression
						plan1.Variable = variable;
						plan1.Expression = new Expression(plan1.Expression, Operator.And, exp);
						return;
					}
				}

				// Didn't find so make a new entry in the list.
				SingleVarPlan plan = new SingleVarPlan();
				plan.TableSource = table;
				plan.Variable = variable;
				plan.SingleVariable = singleVar;
				plan.Expression = exp;
				list.Add(plan);
				return;
			}

			// ----

			// An expression plan for a constant expression.  These are very
			// optimizable indeed.


			/// <summary>
			/// Evaluates a list of constant conditional exressions of 
			/// the form <c>3 + 2 = 0</c>, <c>true = true</c>, etc.
			/// </summary>
			/// <param name="constantVars"></param>
			/// <param name="evaluateOrder"></param>
			private void EvaluateConstants(IList<Expression> constantVars, IList<ExpressionPlan> evaluateOrder) {
				// For each constant variable
				foreach (Expression expr in constantVars) {
					// Add the exression plan
					ExpressionPlan expPlan = new ConstantExpressionPlan(this, expr);
					expPlan.OptimizableValue = 0f;
					evaluateOrder.Add(expPlan);
				}
			}

			/// <summary>
			/// Evaluates a list of single variable conditional expressions 
			/// of the form <c>a = 3</c>, <c>a &gt; 1 + 2</c>, <c>a - 2 = 1</c>, 
			/// <c>3 = a</c>, <c>concat(a, 'a') = '3a'</c>, etc.
			/// </summary>
			/// <param name="singleVars"></param>
			/// <param name="evaluateOrder"></param>
			/// <remarks>
			/// The rule is there must be only one variable, a conditional 
			/// operator, and a constant on one side.
			/// <para>
			/// This method takes the list and modifies the plan as 
			/// necessary.
			/// </para>
			/// </remarks>
			private void EvaluateSingles(IList<Expression> singleVars, IList<ExpressionPlan> evaluateOrder) {
				// The list of simple expression plans (lhs = single)
				List<SingleVarPlan> simplePlanList = new List<SingleVarPlan>();
				// The list of complex function expression plans (lhs = expression)
				List<SingleVarPlan> complexPlanList = new List<SingleVarPlan>();

				// For each single variable expression
				foreach (Expression andexp in singleVars) {
					// The operator
					Operator op = (Operator)andexp.Last;

					// Split the expression
					Expression[] exps = andexp.Split();
					// The single var
					VariableName singleVar;

					// If the operator is a sub-command we must be of the form,
					// 'a in ( 1, 2, 3 )'
					if (op.IsSubQuery) {
						singleVar = exps[0].AsVariableName();
						if (singleVar != null) {
							ExpressionPlan expPlan = new SimpleSelectExpressionPlan(this, singleVar, op, exps[1]);
							expPlan.OptimizableValue = 0.2f;
							evaluateOrder.Add(expPlan);
						} else {
							singleVar = exps[0].AllVariables[0];
							ExpressionPlan expPlan = new ComplexSingleExpressionPlan(this, singleVar, andexp);
							expPlan.OptimizableValue = 0.8f;
							evaluateOrder.Add(expPlan);
						}
					} else {
						// Put the variable on the LHS, constant on the RHS
						IList<VariableName> allVars = exps[0].AllVariables;
						if (allVars.Count == 0) {
							// Reverse the expressions and the operator
							Expression tempExp = exps[0];
							exps[0] = exps[1];
							exps[1] = tempExp;
							op = op.Reverse();
							singleVar = exps[0].AllVariables[0];
						} else {
							singleVar = allVars[0];
						}

						// The table source
						PlanTableSource tableSource = FindTableSource(singleVar);

						// Simple LHS?
						VariableName v = exps[0].AsVariableName();
						if (v != null) {
							AddSingleVarPlanTo(simplePlanList, tableSource, v, singleVar, exps, op);
						} else {
							// No, complex lhs
							AddSingleVarPlanTo(complexPlanList, tableSource, null, singleVar, exps, op);
						}
					}
				}

				// We now have a list of simple and complex plans for each table,
				foreach (SingleVarPlan varPlan in simplePlanList) {
					ExpressionPlan expPlan = new SimpleSingleExpressionPlan(this, varPlan.SingleVariable, varPlan.Expression);
					expPlan.OptimizableValue = 0.2f;
					evaluateOrder.Add(expPlan);
				}

				foreach (SingleVarPlan var_plan in complexPlanList) {
					ExpressionPlan expPlan = new ComplexSingleExpressionPlan(this, var_plan.SingleVariable, var_plan.Expression);
					expPlan.OptimizableValue = 0.8f;
					evaluateOrder.Add(expPlan);
				}
			}

			/// <summary>
			/// Evaluates a list of expressions that are pattern searches (eg. LIKE, 
			/// NOT LIKE and REGEXP).
			/// </summary>
			/// <param name="pattern_exprs"></param>
			/// <param name="evaluateOrder"></param>
			/// <remarks>
			/// The LHS or RHS may be complex expressions with variables, but we are 
			/// guarenteed that there are no sub-expressions in the expression.
			/// </remarks>
			private void EvaluatePatterns(IList<Expression> pattern_exprs, IList<ExpressionPlan> evaluateOrder) {
				// Split the patterns into simple and complex plans.  A complex plan
				// may require that a join occurs.
				foreach (Expression expr in pattern_exprs) {
					Expression[] exps = expr.Split();
					// If the LHS is a single variable and the RHS is a constant then
					// the conditions are right for a simple pattern search.
					VariableName lhsVar = exps[0].AsVariableName();
					if (expr.IsConstant) {
						ExpressionPlan exprPlan = new ConstantExpressionPlan(this, expr);
						exprPlan.OptimizableValue = 0f;
						evaluateOrder.Add(exprPlan);
					} else if (lhsVar != null && exps[1].IsConstant) {
						ExpressionPlan exprPlan = new SimplePatternExpressionPlan(this, lhsVar, expr);
						exprPlan.OptimizableValue = 0.25f;
						evaluateOrder.Add(exprPlan);
					} else {
						// Otherwise we must assume a complex pattern search which may
						// require a join.  For example, 'a + b LIKE 'a%'' or
						// 'a LIKE b'.  At the very least, this will be an exhaustive
						// search and at the worst it will be a join + exhaustive search.
						// So we should evaluate these at the end of the evaluation order.
						ExpressionPlan exprPlan = new ExhaustiveSelectExpressionPlan(this, expr);
						exprPlan.OptimizableValue = 0.82f;
						evaluateOrder.Add(exprPlan);
					}

				}

			}

			/// <summary>
			/// Evaluates a list of expressions containing sub-queries.
			/// </summary>
			/// <param name="expressions"></param>
			/// <param name="evaluateOrder"></param>
			/// <remarks>
			/// Non-correlated sub-queries can often be optimized in to fast 
			/// searches.  Correlated queries, or expressions containing multiple 
			/// sub-queries are write through the <see cref="ExhaustiveSelectExpressionPlan"/>.
			/// </remarks>
			private void EvaluateSubQueries(IList<Expression> expressions, IList<ExpressionPlan> evaluateOrder) {
				// For each sub-command expression
				foreach (Expression andexp in expressions) {
					bool isExhaustive;

					// Is this an easy sub-command?
					Operator op = (Operator)andexp.Last;
					if (op.IsSubQuery) {
						// Split the expression.
						Expression[] exps = andexp.Split();
						// Check that the left is a simple enough variable reference
						VariableName leftVar = exps[0].AsVariableName();
						if (leftVar != null) {
							// Check that the right is a sub-command plan.
							IQueryPlanNode rightPlan = exps[1].AsQueryPlanNode();
							if (rightPlan != null) {
								// Finally, check if the plan is correlated or not
								IList<CorrelatedVariable> cv = rightPlan.DiscoverCorrelatedVariables(1, new List<CorrelatedVariable>());
								if (cv.Count == 0) {
									// No correlated variables so we are a standard, non-correlated
									// command!
									isExhaustive = false;
								} else {
									isExhaustive = true;
								}
							} else {
								isExhaustive = true;
							}
						} else {
							isExhaustive = true;
						}
					} else {
						// Must be an exhaustive sub-command
						isExhaustive = true;
					}

					// If this is an exhaustive operation,
					if (isExhaustive) {
						// This expression could involve multiple variables, so we may need
						// to join.
						IList<VariableName> allVars = andexp.AllVariables;

						// Also find all correlated variables.
						int level = 0;
						IList<CorrelatedVariable> allCorrelated = andexp.DiscoverCorrelatedVariables(ref level,
						                                                                              new List<CorrelatedVariable>());
						int sz = allCorrelated.Count;

						// If there are no variables (and no correlated variables) then this
						// must be a constant select, For example, 3 in ( select ... )
						if (allVars.Count == 0 && sz == 0) {
							ExpressionPlan exprPlan = new ConstantExpressionPlan(this, andexp);
							exprPlan.OptimizableValue = 0f;
							evaluateOrder.Add(exprPlan);
						} else {
							foreach (CorrelatedVariable cv in allCorrelated)
								allVars.Add(cv.VariableName);

							// An exhaustive expression plan which might require a join or a
							// slow correlated search.  This should be evaluated after the
							// multiple variables are processed.
							ExpressionPlan expPlan = new ExhaustiveSubQueryExpressionPlan(this, allVars, andexp);
							expPlan.OptimizableValue = 0.85f;
							evaluateOrder.Add(expPlan);
						}

					} else {
						// This is a simple sub-command expression plan with a single LHS
						// variable and a single RHS sub-command.
						ExpressionPlan expPlan = new SimpleSubQueryExpressionPlan(this, andexp);
						expPlan.OptimizableValue = 0.3f;
						evaluateOrder.Add(expPlan);

					}

				} // For each 'and' expression

			}

			/// <summary>
			/// Evaluates a list of expressions containing multiple variable 
			/// expression in the form <c>a = b</c>, <c>a &gt; b + c</c>, 
			/// <c> + 5 * b = 2</c>, etc.
			/// </summary>
			/// <param name="multiVars"></param>
			/// <param name="evaluateOrder"></param>
			/// <remarks>
			/// If an expression represents a simple join condition then 
			/// a join plan is made to the command plan tree. If an expression 
			/// represents a more complex joining condition then an exhaustive 
			/// search must be used.
			/// </remarks>
			private void EvaluateMultiples(IList<Expression> multiVars, IList<ExpressionPlan> evaluateOrder) {
				// FUTURE OPTIMIZATION:
				//   This join order planner is a little primitive in design.  It orders
				//   optimizable joins first and least optimizable last, but does not
				//   take into account other factors that we could use to optimize
				//   joins in the future.

				// For each single variable expression
				foreach (Expression expr in multiVars) {
					Expression[] exps = expr.Split();

					// Get the list of variables in the left hand and right hand side
					VariableName lhsVar = exps[0].AsVariableName();
					VariableName rhsVar = exps[1].AsVariableName();

					// Work out how optimizable the join is.
					// The calculation is as follows;
					// a) If both the lhs and rhs are a single variable then the
					//    optimizable value is set to 0.6f.
					// b) If only one of lhs or rhs is a single variable then the
					//    optimizable value is set to 0.64f.
					// c) Otherwise it is set to 0.68f (exhaustive select guarenteed).

					if (lhsVar == null && rhsVar == null) {
						// Neither lhs or rhs are single vars
						ExpressionPlan expPlan = new ExhaustiveJoinExpressionPlan(this, expr);
						expPlan.OptimizableValue = 0.68f;
						evaluateOrder.Add(expPlan);
					} else if (lhsVar != null && rhsVar != null) {
						// Both lhs and rhs are a single var (most optimizable type of
						// join).
						ExpressionPlan expPlan = new StandardJoinExpressionPlan(this, expr);
						expPlan.OptimizableValue = 0.60f;
						evaluateOrder.Add(expPlan);
					} else {
						// Either lhs or rhs is a single var
						ExpressionPlan expPlan = new StandardJoinExpressionPlan(this, expr);
						expPlan.OptimizableValue = 0.64f;
						evaluateOrder.Add(expPlan);
					}
				} // for each expression we are 'and'ing against
			}

			/// <summary>
			/// Evaluates a list of expressions that are sub-expressions 
			/// themselves.
			/// </summary>
			/// <param name="sublogicExprs"></param>
			/// <param name="evaluateOrder"></param>
			/// <remarks>
			/// This is typically called when we have OR queries in the 
			/// expression.
			/// </remarks>
			private void EvaluateSubLogic(IList<Expression> sublogicExprs, IList<ExpressionPlan> evaluateOrder) {
				//each_logic_expr:
				foreach (Expression expr in sublogicExprs) {
					// Break the expression down to a list of OR expressions,
					IList<Expression> orExprs = expr.BreakByOperator(new List<Expression>(), "or");

					// An optimizations here;

					// If all the expressions we are ORing together are in the same table
					// then we should execute them before the joins, otherwise they
					// should go after the joins.

					// The reason for this is because if we can lesson the amount of work a
					// join has to do then we should.  The actual time it takes to perform
					// an OR search shouldn't change if it is before or after the joins.

					PlanTableSource common = null;

					foreach (Expression orExpr in orExprs) {
						IList<VariableName> vars = orExpr.AllVariables;
						// If there are no variables then don't bother with this expression
						if (vars.Count > 0) {
							// Find the common table source (if any)
							PlanTableSource ts = FindCommonTableSource(vars);
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
								ExpressionPlan expPlan1 = new SubLogicExpressionPlan(this, expr);
								expPlan1.OptimizableValue = 0.70f;
								evaluateOrder.Add(expPlan1);
								// Continue to the next logic expression
								//TODO: check this...
								goto each_logic_expr;
							}
						}
					}

					// Either we found a common table or there are no variables in the OR.
					// Either way we should evaluate this after the join.
					ExpressionPlan expPlan = new SubLogicExpressionPlan(this, expr);
					expPlan.OptimizableValue = 0.58f;
					evaluateOrder.Add(expPlan);

				each_logic_expr:
					continue;
				}
			}


			// -----

			/// <summary>
			/// Generates a plan to evaluate the given list of expressions
			/// (logically separated with AND).
			/// </summary>
			/// <param name="andList"></param>
			private void PlanForExpressionList(IList<Expression> andList) {
				List<Expression> subLogicExpressions = new List<Expression>();
				// The list of expressions that have a sub-select in them.
				List<Expression> subQueryExpressions = new List<Expression>();
				// The list of all constant expressions ( true = true )
				List<Expression> constants = new List<Expression>();
				// The list of pattern matching expressions (eg. 't LIKE 'a%')
				List<Expression> patternExpressions = new List<Expression>();
				// The list of all expressions that are a single variable on one
				// side, a conditional operator, and a constant on the other side.
				List<Expression> singleVars = new List<Expression>();
				// The list of multi variable expressions (possible joins)
				List<Expression> multiVars = new List<Expression>();

				// Separate out each condition type.
				foreach (Expression andexp in andList) {
					// If we end with a logical operator then we must recurse them
					// through this method.
					object lob = andexp.Last;
					Operator op;
					// If the last is not an operator, then we imply
					// '[expression] = true'
					if (!(lob is Operator) ||
						((Operator)lob).IsMathematical) {
						Operator equalOp = Operator.Equal;
						andexp.AddElement(TObject.CreateBoolean(true));
						andexp.AddOperator(equalOp);
						op = equalOp;
					} else {
						op = (Operator)lob;
					}
					// If the last is logical (eg. AND, OR) then we must process the
					// sub logic expression
					if (op.IsLogical) {
						subLogicExpressions.Add(andexp);
					}
						// Does the expression have a sub-command?  (eg. Another select
						//   statement somewhere in it)
					else if (andexp.HasSubQuery) {
						subQueryExpressions.Add(andexp);
					} else if (op.IsPattern) {
						patternExpressions.Add(andexp);
					} else { //if (op.isCondition()) {
						// The list of variables in the expression.
						IList<VariableName> vars = andexp.AllVariables;
						if (vars.Count == 0) {
							// These are ( 54 + 9 = 9 ), ( "z" > "a" ), ( 9.01 - 2 ), etc
							constants.Add(andexp);
						} else if (vars.Count == 1) {
							// These are ( id = 90 ), ( 'a' < number ), etc
							singleVars.Add(andexp);
						} else if (vars.Count > 1) {
							// These are ( id = part_id ),
							// ( cost_of + value_of < sold_at ), ( id = part_id - 10 )
							multiVars.Add(andexp);
						} else {
							throw new ApplicationException("Hmm, vars list size is negative!");
						}
					}
				}

				// The order in which expression are evaluated,
				// (ExpressionPlan)
				List<ExpressionPlan> evaluateOrder = new List<ExpressionPlan>();

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



				// Sort the evaluation list by how optimizable the expressions are,
				evaluateOrder.Sort();
				// And add each expression to the plan
				foreach (ExpressionPlan plan in evaluateOrder) {
					plan.AddToPlanTree();
				}
			}

			/// <summary>
			/// Evaluates the search Expression clause and alters the 
			/// banches of the plans in this object as necessary.
			/// </summary>
			/// <param name="exp"></param>
			/// <remarks>
			/// Unlike <see cref="LogicalEvaluate"/>, this does not result 
			/// in a single <see cref="IQueryPlanNode"/>.
			/// It is the responsibility of the callee to join branches 
			/// as required.
			/// </remarks>
			private void PlanForExpression(Expression exp) {
				if (exp == null)
					return;

				object ob = exp.Last;
				if (ob is Operator && ((Operator)ob).IsLogical) {
					Operator lastOp = (Operator)ob;

					if (lastOp.IsEquivalent("or")) {
						// parsing an OR block
						// Split left and right of logical operator.
						Expression[] exps = exp.Split();
						// If we are an 'or' then evaluate left and right and union the
						// result.

						// Before we branch set cache points.
						SetCachePoints();

						// Make copies of the left and right planner
						QueryTableSetPlanner leftPlanner = Copy();
						QueryTableSetPlanner rightPlanner = Copy();

						// Plan the left and right side of the OR
						leftPlanner.PlanForExpression(exps[0]);
						rightPlanner.PlanForExpression(exps[1]);

						// Fix the left and right planner so that they represent the same
						// 'group'.
						// The current implementation naturally joins all sources if the
						// number of sources is different than the original size.
						int leftSz = leftPlanner.tableList.Count;
						int rightSz = rightPlanner.tableList.Count;
						if (leftSz != rightSz ||
							leftPlanner.HasJoinOccured ||
							rightPlanner.HasJoinOccured) {
							// Naturally join all in the left and right plan
							leftPlanner.NaturalJoinAll();
							rightPlanner.NaturalJoinAll();
						}

						// Union all table sources, but only if they have changed.
						List<PlanTableSource> leftTableList = leftPlanner.tableList;
						List<PlanTableSource> rightTableList = rightPlanner.tableList;
						int sz = leftTableList.Count;

						// First we must determine the plans that need to be joined in the
						// left and right plan.
						List<PlanTableSource> leftJoinList = new List<PlanTableSource>();
						List<PlanTableSource> rightJoinList = new List<PlanTableSource>();
						for (int i = 0; i < sz; ++i) {
							PlanTableSource leftPlan = leftTableList[i];
							PlanTableSource rightPlan = rightTableList[i];
							if (leftPlan.IsUpdated || rightPlan.IsUpdated) {
								leftJoinList.Add(leftPlan);
								rightJoinList.Add(rightPlan);
							}
						}

						// Make sure the plans are joined in the left and right planners
						leftPlanner.JoinAllPlansToSingleSource(leftJoinList);
						rightPlanner.JoinAllPlansToSingleSource(rightJoinList);

						// Since the planner lists may have changed we update them here.
						leftTableList = leftPlanner.tableList;
						rightTableList = rightPlanner.tableList;
						sz = leftTableList.Count;

						List<PlanTableSource> newTableList = new List<PlanTableSource>(sz);

						for (int i = 0; i < sz; ++i) {
							PlanTableSource leftPlan = leftTableList[i];
							PlanTableSource rightPlan = rightTableList[i];

							PlanTableSource newPlan;

							// If left and right plan updated so we need to union them
							if (leftPlan.IsUpdated || rightPlan.IsUpdated) {
								// In many causes, the left and right branches will contain
								//   identical branches that would best be optimized out.

								// Take the left plan, add the logical union to it, and make it
								// the plan for this.
								IQueryPlanNode node = new LogicalUnionNode(leftPlan.Plan, rightPlan.Plan);

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
						tableList = newTableList;
					} else if (lastOp.IsEquivalent("and")) {
						// parsing an AND block
						// The list of AND expressions that are here
						IList<Expression> andList = CreateAndList(new List<Expression>(), exp);

						PlanForExpressionList(andList);
					} else {
						throw new Exception("Unknown logical operator: " + ob);
					}
				} else {
					// Not a logical expression so just plan for this single expression.
					List<Expression> expList = new List<Expression>(1);
					expList.Add(exp);
					PlanForExpressionList(expList);
				}
			}

			/// <summary>
			/// Evaluates a search Expression clause.
			/// </summary>
			/// <param name="exp"></param>
			/// <remarks>
			/// Note that is some cases this will generate a plan tree 
			/// that has many identical branches that can be optimized out.
			/// </remarks>
			/// <returns></returns>
			private IQueryPlanNode LogicalEvaluate(Expression exp) {
				if (exp == null) {
					// Naturally join everything and return the plan.
					NaturalJoinAll();
					// Return the plan
					return SingleTableSource.Plan;
				}

				// Plan the expression
				PlanForExpression(exp);

				// Naturally join any straggling tables
				NaturalJoinAll();

				// Return the plan
				return SingleTableSource.Plan;
			}


			/// <summary>
			/// Given an Expression, this will return a list of expressions 
			/// that can be safely executed as a set of 'and' operations.
			/// </summary>
			/// <param name="list"></param>
			/// <param name="exp"></param>
			/// <remarks>
			/// For example, an expression of <c>a=9 and b=c and d=2</c> 
			/// would return the list: <i>a=9</i>,<i>b=c</i>, <i>d=2</i>.
			/// <para>
			/// If non 'and' operators are found then the reduction stops.
			/// </para>
			/// </remarks>
			/// <returns></returns>
			private static IList<Expression> CreateAndList(IList<Expression> list, Expression exp) {
				return exp.BreakByOperator(list, "and");
			}

			/// <summary>
			/// Evalutes the WHERE clause of the table expression.
			/// </summary>
			/// <param name="searchExpression"></param>
			/// <returns></returns>
			public IQueryPlanNode PlanSearchExpression(SearchExpression searchExpression) {
				// First perform all outer tables.
				PlanAllOuterJoins();

				IQueryPlanNode node = LogicalEvaluate(searchExpression.FromExpression);
				return node;
			}

			/// <summary>
			/// Makes an exact duplicate copy (deep clone) of this planner object.
			/// </summary>
			/// <returns></returns>
			private QueryTableSetPlanner Copy() {
				QueryTableSetPlanner copy = new QueryTableSetPlanner();
				int sz = tableList.Count;
				for (int i = 0; i < sz; ++i) {
					copy.tableList.Add(tableList[i].Copy());
				}
				// Copy the left and right links in the PlanTableSource
				for (int i = 0; i < sz; ++i) {
					PlanTableSource src = tableList[i];
					PlanTableSource mod = copy.tableList[i];
					// See how the left plan links to which index,
					if (src.LeftPlan != null) {
						int n = IndexOfPlanTableSource(src.LeftPlan);
						mod.SetLeftJoinInfo(copy.tableList[n], src.LeftJoinType, src.LeftOnExpression);
					}
					// See how the right plan links to which index,
					if (src.RightPlan != null) {
						int n = IndexOfPlanTableSource(src.RightPlan);
						mod.SetRightJoinInfo(copy.tableList[n], src.RightJoinType, src.RightOnExpression);
					}
				}

				return copy;
			}

			/// <summary>
			/// Convenience class that stores an expression to evaluate for a table.
			/// </summary>
			private sealed class SingleVarPlan {
				public PlanTableSource TableSource;
				public VariableName SingleVariable;
				public VariableName Variable;
				public Expression Expression;
			}

			private class ConstantExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly Expression expression;

				public ConstantExpressionPlan(QueryTableSetPlanner qtsp, Expression e) {
					this.qtsp = qtsp;
					expression = e;
				}

				public override void AddToPlanTree() {
					// Each currently open branch must have this constant expression added
					// to it.
					foreach (PlanTableSource plan in qtsp.tableList)
						plan.UpdatePlan(new ConstantSelectNode(plan.Plan, expression));
				}
			}

			private class SimpleSelectExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly VariableName singleVar;
				private readonly Operator op;
				private readonly Expression expression;

				public SimpleSelectExpressionPlan(QueryTableSetPlanner qtsp, VariableName singleVar, Operator op, Expression expression) {
					this.qtsp = qtsp;
					this.singleVar = singleVar;
					this.op = op;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					// Find the table source for this variable
					PlanTableSource tableSource = qtsp.FindTableSource(singleVar);
					tableSource.UpdatePlan(new SimpleSelectNode(tableSource.Plan, singleVar, op, expression));
				}
			}

			private class SimpleSingleExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly VariableName singleVar;
				private readonly Expression expression;

				public SimpleSingleExpressionPlan(QueryTableSetPlanner qtsp, VariableName singleVar, Expression expression) {
					this.qtsp = qtsp;
					this.singleVar = singleVar;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					// Find the table source for this variable
					PlanTableSource tableSource = qtsp.FindTableSource(singleVar);
					tableSource.UpdatePlan(new RangeSelectNode(tableSource.Plan, expression));
				}
			}

			private class ComplexSingleExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly VariableName singleVar;
				private readonly Expression expression;

				public ComplexSingleExpressionPlan(QueryTableSetPlanner qtsp, VariableName singleVar, Expression expression) {
					this.qtsp = qtsp;
					this.singleVar = singleVar;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					// Find the table source for this variable
					PlanTableSource tableSource = qtsp.FindTableSource(singleVar);
					tableSource.UpdatePlan(new ExhaustiveSelectNode(tableSource.Plan, expression));
				}
			}

			private class SimplePatternExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly VariableName singleVar;
				private readonly Expression expression;

				public SimplePatternExpressionPlan(QueryTableSetPlanner qtsp, VariableName singleVar, Expression expression) {
					this.qtsp = qtsp;
					this.singleVar = singleVar;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					// Find the table source for this variable
					PlanTableSource tableSource = qtsp.FindTableSource(singleVar);
					tableSource.UpdatePlan(new SimplePatternSelectNode(tableSource.Plan, expression));
				}
			}

			private class ExhaustiveSelectExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly Expression expression;

				public ExhaustiveSelectExpressionPlan(QueryTableSetPlanner qtsp, Expression expression) {
					this.qtsp = qtsp;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					// Get all the variables of this expression.
					IList<VariableName> allVars = expression.AllVariables;
					// Find the table source for this set of variables.
					PlanTableSource tableSource = qtsp.JoinAllPlansWithVariables(allVars);
					// Perform the exhaustive select
					tableSource.UpdatePlan(new ExhaustiveSelectNode(tableSource.Plan, expression));
				}
			}

			private class ExhaustiveSubQueryExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly IList<VariableName> allVars;
				private readonly Expression expression;

				public ExhaustiveSubQueryExpressionPlan(QueryTableSetPlanner qtsp, IList<VariableName> allVars, Expression expression) {
					this.qtsp = qtsp;
					this.allVars = allVars;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					PlanTableSource tableSource = qtsp.JoinAllPlansWithVariables(allVars);
					// Update the plan
					tableSource.UpdatePlan(new ExhaustiveSelectNode(tableSource.Plan, expression));

				}
			}

			private class SimpleSubQueryExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly Expression expression;

				public SimpleSubQueryExpressionPlan(QueryTableSetPlanner qtsp, Expression expression) {
					this.qtsp = qtsp;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					Operator op = (Operator)expression.Last;
					Expression[] exps = expression.Split();
					VariableName leftVar = exps[0].AsVariableName();
					IQueryPlanNode rightPlan = exps[1].AsQueryPlanNode();

					// Find the table source for this variable
					PlanTableSource tableSource = qtsp.FindTableSource(leftVar);
					// The left branch
					IQueryPlanNode leftPlan = tableSource.Plan;
					// Update the plan
					tableSource.UpdatePlan(new NonCorrelatedAnyAllNode(leftPlan, rightPlan, new VariableName[] { leftVar }, op));
				}
			}

			private class ExhaustiveJoinExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly Expression expression;

				public ExhaustiveJoinExpressionPlan(QueryTableSetPlanner qtsp, Expression expression) {
					this.qtsp = qtsp;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					// Get all the variables in the expression
					IList<VariableName> allVars = expression.AllVariables;
					// Merge it into one plan (possibly performing natural joins).
					PlanTableSource allPlan = qtsp.JoinAllPlansWithVariables(allVars);
					// And perform the exhaustive select,
					allPlan.UpdatePlan(new ExhaustiveSelectNode(allPlan.Plan, expression));
				}
			}

			private class StandardJoinExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly Expression expression;

				public StandardJoinExpressionPlan(QueryTableSetPlanner qtsp, Expression expression) {
					this.qtsp = qtsp;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					// Get the expression with the multiple variables
					Expression[] exps = expression.Split();

					// Get the list of variables in the left hand and right hand side
					VariableName lhsVar = exps[0].AsVariableName();
					VariableName rhsVar = exps[1].AsVariableName();
					IList<VariableName> lhsVars = exps[0].AllVariables;
					IList<VariableName> rhsVars = exps[1].AllVariables;

					// Get the operator
					Operator op = (Operator)expression.Last;

					// Get the left and right plan for the variables in the expression.
					// Note that these methods may perform natural joins on the table.
					PlanTableSource lhsPlan = qtsp.JoinAllPlansWithVariables(lhsVars);
					PlanTableSource rhsPlan = qtsp.JoinAllPlansWithVariables(rhsVars);

					// If the lhs and rhs plans are different (there is a joining
					// situation).
					if (lhsPlan != rhsPlan) {
						// If either the LHS or the RHS is a single variable then we can
						// optimize the join.

						if (lhsVar != null || rhsVar != null) {
							// If rhs_v is a single variable and lhs_v is not then we must
							// reverse the expression.
							JoinNode joinNode;
							if (lhsVar == null) {
								// Reverse the expressions and the operator
								joinNode = new JoinNode(rhsPlan.Plan, lhsPlan.Plan, rhsVar, op.Reverse(), exps[0]);
								qtsp.MergeTables(rhsPlan, lhsPlan, joinNode);
							} else {
								// Otherwise, use it as it is.
								joinNode = new JoinNode(lhsPlan.Plan, rhsPlan.Plan, lhsVar, op, exps[1]);
								qtsp.MergeTables(lhsPlan, rhsPlan, joinNode);
							}

							// Return because we are done
							return;
						}
					} // if lhs and rhs plans are different

					// If we get here either both the lhs and rhs are complex expressions
					// or the lhs and rhs of the variable are not different plans, or
					// the operator is not a conditional.  Either way, we must evaluate
					// this via a natural join of the variables involved coupled with an
					// exhaustive select.  These types of queries are poor performing.

					// Get all the variables in the expression
					IList<VariableName> allVars = expression.AllVariables;
					// Merge it into one plan (possibly performing natural joins).
					PlanTableSource allPlan = qtsp.JoinAllPlansWithVariables(allVars);
					// And perform the exhaustive select,
					allPlan.UpdatePlan(new ExhaustiveSelectNode(allPlan.Plan, expression));
				}
			}

			private class SubLogicExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly Expression expression;

				public SubLogicExpressionPlan(QueryTableSetPlanner qtsp, Expression expression) {
					this.qtsp = qtsp;
					this.expression = expression;
				}

				public override void AddToPlanTree() {
					qtsp.PlanForExpression(expression);
				}
			}
		} 
	}
}