// 
//  Copyright 2010-2014 Deveel
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
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	internal sealed class QueryTableSetPlanner {
		/// <summary>
		/// The list of <see cref="PlanTableSource"/> objects for each source being planned.
		/// </summary>
		private List<PlanTableSource> tableList;

		public QueryTableSetPlanner() {
			tableList = new List<PlanTableSource>();
		}

		/// <summary>
		/// Returns the single <see cref="PlanTableSource"/> for this planner.
		/// </summary>
		public PlanTableSource SingleTableSource {
			get {
				if (tableList.Count != 1)
					throw new Exception("Not a single table source.");

				return tableList[0];
			}
		}

		/// <summary>
		/// Returns true if a join has occurred ('table_list' has been modified).
		/// </summary>
		public bool HasJoinOccurred { get; private set; }

		public IEnumerable<PlanTableSource> Sources {
			get { return tableList.AsReadOnly(); }
		}

		/// <summary>
		/// Add a <see cref="PlanTableSource"/> to this planner.
		/// </summary>
		/// <param name="source"></param>
		public void AddPlanTableSource(PlanTableSource source) {
			tableList.Add(source);
			HasJoinOccurred = true;
		}

		/// <summary>
		/// Adds a new table source to the planner given a Plan that 'creates'
		/// the source.
		/// </summary>
		/// <param name="plan"></param>
		/// <param name="fromTable">The <see cref="IFromTableSource"/> that describes the source 
		/// created by the plan.</param>
		public void AddTableSource(QueryPlanNode plan, IFromTableSource fromTable) {
			var allCols = fromTable.AllColumns;
			var uniqueNames = new[] {fromTable.UniqueName};
			AddPlanTableSource(new PlanTableSource(plan, allCols, uniqueNames));
		}

		/// <summary>
		/// Returns the index of the given <see cref="PlanTableSource"/> in the 
		/// table list.
		/// </summary>
		/// <param name="source"></param>
		/// <returns></returns>
		public int IndexOfPlanTableSource(PlanTableSource source) {
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
		public void SetJoinBetweenSources(int betweenIndex, JoinType joinType, SqlExpression onExpression) {
			PlanTableSource planLeft = tableList[betweenIndex];
			PlanTableSource planRight = tableList[betweenIndex + 1];
			planLeft.SetRightJoin(planRight, joinType, onExpression);
			planRight.SetLeftJoin(planLeft, joinType, onExpression);
		}

		/// <summary>
		/// Forms a new PlanTableSource that's the concatination of the given two 
		/// <see cref="PlanTableSource"/> objects.
		/// </summary>
		/// <param name="left"></param>
		/// <param name="right"></param>
		/// <param name="plan"></param>
		/// <returns></returns>
		private static PlanTableSource ConcatTableSources(PlanTableSource left, PlanTableSource right, QueryPlanNode plan) {
			// Merge the variable list
			var newVarList = new ObjectName[left.Variables.Length + right.Variables.Length];
			int i = 0;
			foreach (ObjectName name in left.Variables) {
				newVarList[i++] = name;
			}
			foreach (ObjectName name in right.Variables) {
				newVarList[i++] = name;
			}

			// Merge the unique table names list
			var newUniqueList = new string[left.UniqueNames.Length + right.UniqueNames.Length];
			i = 0;
			foreach (string uniqueName in left.UniqueNames) {
				newUniqueList[i++] = uniqueName;
			}
			foreach (string uniqueName in right.UniqueNames) {
				newUniqueList[i++] = uniqueName;
			}

			// Return the new table source plan.
			return new PlanTableSource(plan, newVarList, newUniqueList);
		}

		public PlanTableSource MergeTables(PlanTableSource left, PlanTableSource right, QueryPlanNode mergePlan) {
			// Remove the sources from the table list.
			tableList.Remove(left);
			tableList.Remove(right);
			// Add the concatination of the left and right tables.
			var concatPlan = ConcatTableSources(left, right, mergePlan);
			concatPlan.SetJoinMergedBetween(left, right);
			concatPlan.IsUpdated = true;
			AddPlanTableSource(concatPlan);
			// Return the name plan
			return concatPlan;
		}

		/// <summary>
		/// Finds and returns the PlanTableSource in the list of tables that
		/// contains the given <see cref="ObjectName"/> reference.
		/// </summary>
		/// <param name="reference"></param>
		/// <returns></returns>
		public PlanTableSource FindTableSource(ObjectName reference) {
			// If there is only 1 plan then assume the variable is in there.
			if (tableList.Count == 1)
				return tableList[0];

			foreach (PlanTableSource source in tableList) {
				if (source.HasVariable(reference))
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
		private PlanTableSource FindCommonTableSource(IList<ObjectName> variables) {
			if (variables.Count == 0)
				return null;

			PlanTableSource plan = FindTableSource(variables[0]);
			int i = 1;
			int sz = variables.Count;
			while (i < sz) {
				PlanTableSource p2 = FindTableSource(variables[i]);
				if (plan != p2)
					return null;

				++i;
			}

			return plan;
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
		private PlanTableSource JoinAllPlansWithVariables(IEnumerable<ObjectName> allVars) {
			// Collect all the plans that encapsulate these variables.
			var touchedPlans = new List<PlanTableSource>();
			foreach (ObjectName v in allVars) {
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
			var workingPlanList = new List<PlanTableSource>(allPlans);

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
			SqlExpression onExpr;
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
				QueryPlanNode node1 = new NaturalJoinNode(plan1.Plan, plan2.Plan);
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
			var planner = new QueryTableSetPlanner();
			planner.AddPlanTableSource(leftPlan.Copy());
			planner.AddPlanTableSource(rightPlan.Copy());

			// Evaluate the on expression
			QueryPlanNode node = planner.LogicalEvaluate(onExpr);
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
			var workingPlanList = new List<PlanTableSource>(tableList);

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
		/// <param name="expressionType"></param>
		private static void AddSingleVarPlanTo(IList<SingleVarPlan> list, PlanTableSource table, ObjectName variable,
			ObjectName singleVar, SqlExpression[] expParts, SqlExpressionType expressionType) {
			var exp = SqlExpression.Binary(expParts[0], expressionType, expParts[1]);
			// Is this source in the list already?
			foreach (SingleVarPlan plan in list) {
				if (plan.TableSource == table &&
				    (variable == null ||
				     plan.Variable.Equals(variable))) {
					// Append to end of current expression
					plan.Variable = variable;
					plan.Expression = SqlExpression.And(plan.Expression, exp);
					return;
				}
			}

			// Didn't find so make a new entry in the list.
			list.Add(new SingleVarPlan {
				TableSource = table,
				Variable = variable,
				SingleVariable = singleVar,
				Expression = exp
			});
		}

		/// <summary>
		/// Evaluates a list of constant conditional exressions of 
		/// the form <c>3 + 2 = 0</c>, <c>true = true</c>, etc.
		/// </summary>
		/// <param name="constantVars"></param>
		/// <param name="evaluateOrder"></param>
		public void EvaluateConstants(IEnumerable<SqlExpression> constantVars, IList<IExpressionPlan> evaluateOrder) {
			// For each constant variable
			foreach (SqlExpression expr in constantVars) {
				// Add the exression plan
				var expPlan = new ConstantExpressionPlan(this, expr);
				expPlan.OptimizeFactor = 0f;
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
		public void EvaluateSingles(IEnumerable<SqlBinaryExpression> singleVars, IList<IExpressionPlan> evaluateOrder) {
			// The list of simple expression plans (lhs = single)
			var simplePlanList = new List<SingleVarPlan>();
			// The list of complex function expression plans (lhs = expression)
			var complexPlanList = new List<SingleVarPlan>();

			// For each single variable expression
			foreach (SqlBinaryExpression andexp in singleVars) {
				var type = andexp.ExpressionType;
				var left = andexp.Left;
				var right = andexp.Right;

				// The single var
				ObjectName singleVar;

				// If the operator is a sub-command we must be of the form,
				// 'a in ( 1, 2, 3 )'
				if (type == SqlExpressionType.Any ||
				    type == SqlExpressionType.All) {
					if (left is SqlReferenceExpression) {
						singleVar = ((SqlReferenceExpression) left).ReferenceName;
						ExpressionPlan expPlan = new SimpleSelectExpressionPlan(this, singleVar, type, right);
						expPlan.OptimizeFactor = 0.2f;
						evaluateOrder.Add(expPlan);
					} else {
						singleVar = left.AllReferences().First();
						ExpressionPlan expPlan = new ComplexSingleExpressionPlan(this, singleVar, andexp);
						expPlan.OptimizeFactor = 0.8f;
						evaluateOrder.Add(expPlan);
					}
				} else {
					// Put the variable on the LHS, constant on the RHS
					var allVars = left.AllReferences();
					if (!allVars.Any()) {
						// Reverse the expressions and the operator
						SqlExpression tempExp = left;
						left = right;
						right = tempExp;
						type = andexp.ReverseType;
						singleVar = left.AllReferences().First();
					} else {
						singleVar = allVars.First();
					}

					// The table source
					PlanTableSource tableSource = FindTableSource(singleVar);

					// Simple LHS?
					if (left is SqlReferenceExpression) {
						var reference = ((SqlReferenceExpression) left).ReferenceName;
						AddSingleVarPlanTo(simplePlanList, tableSource, reference, singleVar, new[] {left, right}, type);
					} else {
						// No, complex lhs
						AddSingleVarPlanTo(complexPlanList, tableSource, null, singleVar, new[] {left, right}, type);
					}
				}
			}

			// We now have a list of simple and complex plans for each table,
			foreach (SingleVarPlan varPlan in simplePlanList) {
				ExpressionPlan expPlan = new SimpleSingleExpressionPlan(this, varPlan.SingleVariable, varPlan.Expression);
				expPlan.OptimizeFactor = 0.2f;
				evaluateOrder.Add(expPlan);
			}

			foreach (SingleVarPlan varPlan in complexPlanList) {
				ExpressionPlan expPlan = new ComplexSingleExpressionPlan(this, varPlan.SingleVariable, varPlan.Expression);
				expPlan.OptimizeFactor = 0.8f;
				evaluateOrder.Add(expPlan);
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
		private QueryPlanNode LogicalEvaluate(SqlExpression exp) {
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

		private void PlanForExpression(SqlExpression expression) {
			throw new NotImplementedException();
		}

		#region ExpressionPlan

		abstract class ExpressionPlan : IExpressionPlan {
			protected ExpressionPlan(QueryTableSetPlanner planner) {
				TableSetPlanner = planner;
			}

			protected QueryTableSetPlanner TableSetPlanner { get; private set; }

			public float OptimizeFactor { get; set; }

			public int CompareTo(IExpressionPlan other) {
				return OptimizeFactor.CompareTo(other.OptimizeFactor);
			}

			public abstract void AddToPlanTree();
		}

		#endregion

		#region SimpleSelectExpressionPlan

		class SimpleSelectExpressionPlan : ExpressionPlan {
			private readonly ObjectName singleVar;
			private readonly SqlExpressionType op;
			private readonly SqlExpression expression;

			public SimpleSelectExpressionPlan(QueryTableSetPlanner qtsp, ObjectName singleVar, SqlExpressionType op,
				SqlExpression expression)
				: base(qtsp) {
				this.singleVar = singleVar;
				this.op = op;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				// Find the table source for this variable
				var tableSource = TableSetPlanner.FindTableSource(singleVar);
				tableSource.UpdatePlan(new SimpleSelectNode(tableSource.Plan, singleVar, op, expression));
			}
		}

		#endregion

		#region SimpleSingleExpressionPlan

		class SimpleSingleExpressionPlan : ExpressionPlan {
			private readonly ObjectName singleVar;
			private readonly SqlExpression expression;

			public SimpleSingleExpressionPlan(QueryTableSetPlanner qtsp, ObjectName singleVar, SqlExpression expression)
				: base(qtsp) {
				this.singleVar = singleVar;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				// Find the table source for this variable
				var tableSource = TableSetPlanner.FindTableSource(singleVar);
				tableSource.UpdatePlan(new RangeSelectNode(tableSource.Plan, expression));
			}
		}

		#endregion

		#region ConstantExpressionPlan

		class ConstantExpressionPlan : ExpressionPlan {
			private readonly SqlExpression expression;

			public ConstantExpressionPlan(QueryTableSetPlanner planner, SqlExpression expression)
				: base(planner) {
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				// Each currently open branch must have this constant expression added
				// to it.
				foreach (var plan in TableSetPlanner.Sources)
					plan.UpdatePlan(new ConstantSelectNode(plan.Plan, expression));
			}
		}

		#endregion

		#region ComplexSingleExpressionPlan

		class ComplexSingleExpressionPlan : ExpressionPlan {
			private readonly ObjectName singleVar;
			private readonly SqlExpression expression;

			public ComplexSingleExpressionPlan(QueryTableSetPlanner planner, ObjectName singleVar, SqlExpression expression)
				: base(planner) {
				this.singleVar = singleVar;
				this.expression = expression;
			}

			public override void AddToPlanTree() {
				// Find the table source for this variable
				var tableSource = TableSetPlanner.FindTableSource(singleVar);
				tableSource.UpdatePlan(new ExhaustiveSelectNode(tableSource.Plan, expression));
			}
		}

		#endregion
	}
}