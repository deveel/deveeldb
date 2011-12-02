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
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data {
	/// <summary>
	///  Various helper methods for constructing a plan tree, and the plan node
	/// implementations themselves.
	/// </summary>
	class QueryPlan {
		/// <summary>
		/// Replaces all elements of the array with clone versions of
		/// themselves.
		/// </summary>
		/// <param name="array"></param>
		private static void CloneArray(VariableName[] array) {
			if (array != null) {
				for (int i = 0; i < array.Length; ++i) {
					array[i] = (VariableName)array[i].Clone();
				}
			}
		}

		/// <summary>
		/// Replaces all elements of the array with clone versions of
		/// themselves.
		/// </summary>
		/// <param name="array"></param>
		private static void CloneArray(Expression[] array) {
			if (array != null) {
				for (int i = 0; i < array.Length; ++i) {
					array[i] = (Expression)array[i].Clone();
				}
			}
		}

		private static void Indent(int level, StringBuilder buf) {
			for (int i = 0; i < level; ++i) {
				buf.Append(' ');
			}
		}



		// ---------- Plan node implementations ----------

		/// <summary>
		/// A <see cref="IQueryPlanNode"/> with a single child.
		/// </summary>
		[Serializable]
		public abstract class SingleQueryPlanNode : IQueryPlanNode {
			/// <summary>
			/// The single child node.
			/// </summary>
			protected IQueryPlanNode child;

			protected SingleQueryPlanNode(IQueryPlanNode child) {
				this.child = child;
			}

			/// <summary>
			/// Gets the single child node of the plan.
			/// </summary>
			public IQueryPlanNode Child {
				get { return child; }
			}

			/// <inheritdoc/>
			public abstract Table Evaluate(IQueryContext context);

			/// <inheritdoc/>
			public virtual IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return child.DiscoverTableNames(list);
			}

			/// <inheritdoc/>
			public virtual IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return child.DiscoverCorrelatedVariables(level, list);
			}

			/// <inheritdoc/>
			public virtual Object Clone() {
				SingleQueryPlanNode node = (SingleQueryPlanNode)MemberwiseClone();
				node.child = (IQueryPlanNode)child.Clone();
				return node;
			}

			public virtual string Title {
				get { return GetType().Name; }
			}

			/// <inheritdoc/>
			public void DebugString(int level, StringBuilder buf) {
				Indent(level, buf);
				buf.Append(Title);
				buf.Append('\n');
				child.DebugString(level + 2, buf);
			}

		}

		/// <summary>
		/// A <see cref="IQueryPlanNode"/> implementation that is a branch with 
		/// two child nodes.
		/// </summary>
		[Serializable]
		public abstract class BranchQueryPlanNode : IQueryPlanNode {
			// The left and right node.
			protected IQueryPlanNode left, right;

			protected BranchQueryPlanNode(IQueryPlanNode left, IQueryPlanNode right) {
				this.left = left;
				this.right = right;
			}

			/// <summary>
			/// Gets the left node of the branch query plan node.
			/// </summary>
			public IQueryPlanNode Left {
				get { return left; }
			}

			/// <summary>
			/// Gets the right node of the branch query plan node.
			/// </summary>
			public IQueryPlanNode Right {
				get { return right; }
			}

			/// <inheritdoc/>
			public abstract Table Evaluate(IQueryContext context);

			/// <inheritdoc/>
			public virtual IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return right.DiscoverTableNames(
					left.DiscoverTableNames(list));
			}

			/// <inheritdoc/>
			public virtual IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return right.DiscoverCorrelatedVariables(level,
						 left.DiscoverCorrelatedVariables(level, list));
			}

			/// <inheritdoc/>
			public virtual Object Clone() {
				BranchQueryPlanNode node = (BranchQueryPlanNode)MemberwiseClone();
				node.left = (IQueryPlanNode)left.Clone();
				node.right = (IQueryPlanNode)right.Clone();
				return node;
			}

			public virtual string Title {
				get { return GetType().Name; }
			}

			/// <inheritdoc/>
			public virtual void DebugString(int level, StringBuilder buf) {
				Indent(level, buf);
				buf.Append(Title);
				buf.Append('\n');
				left.DebugString(level + 2, buf);
				right.DebugString(level + 2, buf);
			}

		}


		/// <summary>
		/// The node for fetching a table from the current transaction.
		/// </summary>
		/// <remarks>
		/// This is a tree node and has no children.
		/// </remarks>
		[Serializable]
		public class FetchTableNode : IQueryPlanNode {
			/// <summary>
			/// The name of the table to fetch.
			/// </summary>
			private readonly TableName table_name;

			/// <summary>
			/// The name to alias the table as.
			/// </summary>
			private readonly TableName alias_name;

			public FetchTableNode(TableName table_name, TableName aliased_as) {
				this.table_name = table_name;
				alias_name = aliased_as;
			}

			public virtual IList<TableName> DiscoverTableNames(IList<TableName> list) {
				if (!list.Contains(table_name)) {
					list.Add(table_name);
				}
				return list;
			}

			/// <inheritdoc/>
			public virtual Table Evaluate(IQueryContext context) {
				// MILD HACK: Cast the context to a DatabaseQueryContext
				DatabaseQueryContext db_context = (DatabaseQueryContext)context;
				DataTable t = db_context.GetTable(table_name);
				if (alias_name != null) {
					return new ReferenceTable(t, alias_name);
				}
				return t;
			}

			/// <inheritdoc/>
			public virtual IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return list;
			}

			/// <inheritdoc/>
			public virtual Object Clone() {
				return MemberwiseClone();
			}

			public virtual string Title {
				get { return "FETCH: " + table_name + " AS " + alias_name; }
			}

			/// <inheritdoc/>
			public void DebugString(int level, StringBuilder buf) {
				Indent(level, buf);
				buf.Append(Title);
				buf.Append('\n');
			}

		}

		/// <summary>
		/// A node for creating a table with a single row.
		/// </summary>
		/// <remarks>
		/// This table is useful for queries that have no underlying row. 
		/// For example, a pure functional table expression.
		/// </remarks>
		[Serializable]
		public class SingleRowTableNode : IQueryPlanNode {
			/// <inheritdoc/>
			public virtual IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return list;
			}

			/// <inheritdoc/>
			public virtual Table Evaluate(IQueryContext context) {
				// MILD HACK: Cast the context to a DatabaseQueryContext
				DatabaseQueryContext db_context = (DatabaseQueryContext)context;
				return db_context.Database.SingleRowTable;
			}

			/// <inheritdoc/>
			public virtual IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return list;
			}

			/// <inheritdoc/>
			public virtual Object Clone() {
				return MemberwiseClone();
			}

			public virtual string Title {
				get { return "SINGLE ROW"; }
			}

			/// <inheritdoc/>
			public void DebugString(int level, StringBuilder buf) {
				Indent(level, buf);
				buf.Append(Title);
				buf.Append('\n');
			}

		}

		/// <summary>
		/// The node that fetches a view from the current session.
		/// </summary>
		/// <remarks>
		/// This is a tree node that has no children, however the child can 
		/// be created by calling <see cref="CreateViewChildNode"/>. This node 
		/// can be removed from a plan tree by calling <see cref="CreateViewChildNode"/>
		/// and substituting this node with the returned child. 
		/// For a planner that normalizes and optimizes plan trees, this is 
		/// a useful feature.
		/// </remarks>
		[Serializable]
		public class FetchViewNode : IQueryPlanNode {
			/// <summary>
			/// The name of the view to fetch.
			/// </summary>
			private readonly TableName table_name;

			/// <summary>
			/// The name to alias the table as.
			/// </summary>
			private readonly TableName alias_name;

			public FetchViewNode(TableName table_name, TableName aliased_as) {
				this.table_name = table_name;
				alias_name = aliased_as;
			}

			/// <summary>
			/// Looks up the query plan in the given context.
			/// </summary>
			/// <param name="context"></param>
			/// <returns>
			/// Returns the <see cref="IQueryPlanNode"/> that resolves to the view.
			/// </returns>
			public virtual IQueryPlanNode CreateViewChildNode(IQueryContext context) {
				DatabaseQueryContext db = (DatabaseQueryContext)context;
				return db.CreateViewQueryPlanNode(table_name);
			}

			/// <inheritdoc/>
			public virtual IList<TableName> DiscoverTableNames(IList<TableName> list) {
				if (!list.Contains(table_name)) {
					list.Add(table_name);
				}
				return list;
			}

			/// <inheritdoc/>
			public virtual Table Evaluate(IQueryContext context) {
				// Create the view child node
				IQueryPlanNode node = CreateViewChildNode(context);
				// Evaluate the plan
				Table t = node.Evaluate(context);

				if (alias_name != null)
					return new ReferenceTable(t, alias_name);
				return t;
			}

			/// <inheritdoc/>
			public virtual IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return list;
			}

			/// <inheritdoc/>
			public virtual Object Clone() {
				return MemberwiseClone();
			}

			/// <inheritdoc/>
			public virtual String titleString() {
				return "VIEW: " + table_name + " AS " + alias_name;
			}

			/// <inheritdoc/>
			public void DebugString(int level, StringBuilder buf) {
				Indent(level, buf);
				buf.Append(titleString());
				buf.Append('\n');
			}

		}

		/// <summary>
		/// The node for performing a simple indexed query on a single column 
		/// of the child node.
		/// </summary>
		/// <remarks>
		/// Finds the set from the child node that matches the range.
		/// <para>
		/// The given <see cref="Expression"/> object must conform to a number of 
		/// rules. It may reference only one column in the child node. It must 
		/// consist of only simple mathemetical and logical operators (&lt;, &gt;, 
		/// =, &lt;&gt;, &gt;=, &lt;=, AND, OR).
		/// The left side of each mathematical operator must be a variable, 
		/// and the right side must be a constant (parameter subsitution or 
		/// correlated value).
		/// </para>
		/// <para>
		/// Breaking any of these rules will mean the range select can not 
		/// happen.
		/// </para>
		/// </remarks>
		/// <example>
		/// For example:
		/// <code>
		/// (col &gt; 10 AND col &lt; 100) OR col &gt; 1000 OR col == 10
		/// </code>
		/// </example>
		[Serializable]
		public class RangeSelectNode : SingleQueryPlanNode {
			/// <summary>
			/// A simple expression that represents the range to select.  See the
			/// class comments for a description for how this expression must be
			/// formed.
			/// </summary>
			private Expression expression;

			public RangeSelectNode(IQueryPlanNode child, Expression exp)
				: base(child) {
				expression = exp;
			}

			/// <summary>
			/// Splits the given expression by the <i>and</i> operator.
			/// </summary>
			/// <param name="list"></param>
			/// <param name="exp"></param>
			/// <remarks>
			/// For example, an expression of <c>a=9 and b=c and d=2</c> 
			/// would return the list: <c>a=9</c>,<c>b=c</c>, <c>d=2</c>.
			/// <para>
			/// If non <i>and</i> operators are found then the reduction stops.
			/// </para>
			/// </remarks>
			/// <returns>
			/// Returns a list of expressions that can be safely executed as 
			/// a set of <i>and</i> operations.
			/// </returns>
			private static IList CreateAndList(IList list, Expression exp) {
				return exp.BreakByOperator(list, "and");
			}

			/// <summary>
			/// Updates a range with the given expression.
			/// </summary>
			/// <param name="context"></param>
			/// <param name="range"></param>
			/// <param name="field"></param>
			/// <param name="e"></param>
			private static void UpdateRange(IQueryContext context, SelectableRangeSet range, DataTableColumnDef field, Expression e) {
				Operator op = (Operator)e.Last;
				Expression[] exps = e.Split();
				// Evaluate to an object
				TObject cell = exps[1].Evaluate(null, null, context);

				// If the evaluated object is not of a comparable type, then it becomes
				// null.
				TType field_type = field.TType;
				if (!cell.TType.IsComparableType(field_type)) {
					cell = new TObject(field_type, null);
				}

				// Intersect this in the range set
				range.Intersect(op, cell);
			}

			/// <summary>
			/// Calculates a list of <see cref="SelectableRange"/> objects that represent 
			/// the range of the expression.
			/// </summary>
			/// <param name="context"></param>
			/// <param name="field"></param>
			/// <param name="range"></param>
			/// <param name="exp"></param>
			private static void CalcRange(IQueryContext context,
								   DataTableColumnDef field,
								   SelectableRangeSet range,
								   Expression exp) {
				Operator op = (Operator)exp.Last;
				if (op.IsLogical) {
					if (op.Is("and")) {
						IList andList = CreateAndList(new ArrayList(), exp);
						int sz = andList.Count;
						for (int i = 0; i < sz; ++i) {
							UpdateRange(context, range, field, (Expression)andList[i]);
						}
					} else if (op.Is("or")) {
						// Split left and right of logical operator.
						Expression[] exps = exp.Split();
						// Calculate the range of the left and right
						SelectableRangeSet left = new SelectableRangeSet();
						CalcRange(context, field, left, exps[0]);
						SelectableRangeSet right = new SelectableRangeSet();
						CalcRange(context, field, right, exps[1]);

						// Union the left and right range with the current range
						range.Union(left);
						range.Union(right);
					} else {
						throw new ApplicationException("Unrecognised logical operator.");
					}
				} else {
					// Not an operator so this is the value.
					UpdateRange(context, range, field, exp);
				}

			}

			/// <inheritdoc/>
			public override Table Evaluate(IQueryContext context) {
				Table t = child.Evaluate(context);

				Expression exp = expression;

				// Assert that all variables in the expression are identical.
				IList all_vars = exp.AllVariables;
				VariableName v = null;
				int sz = all_vars.Count;
				for (int i = 0; i < sz; ++i) {
					VariableName cv = (VariableName)all_vars[i];
					if (v != null) {
						if (!cv.Equals(v)) {
							throw new ApplicationException("Assertion failed: " +
											"Range plan does not contain common variable.");
						}
					}
					v = cv;
				}

				// Find the variable field in the table.
				int col = t.FindFieldName(v);
				if (col == -1) {
					throw new ApplicationException("Couldn't find column reference in table: " + v);
				}
				DataTableColumnDef field = t.GetColumnDef(col);
				// Calculate the range
				SelectableRangeSet range = new SelectableRangeSet();
				CalcRange(context, field, range, exp);

				//      Console.Out.WriteLine("RANGE: ");
				//      Console.Out.WriteLine(range);

				// Select the range from the table
				SelectableRange[] ranges = range.ToArray();
				return t.RangeSelect(v, ranges);

			}

			/// <inheritdoc/>
			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return expression.DiscoverTableNames(base.DiscoverTableNames(list));
			}

			/// <inheritdoc/>
			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				//      Console.Out.WriteLine(expression);
				return expression.DiscoverCorrelatedVariables(ref level,
						 base.DiscoverCorrelatedVariables(level, list));
			}

			/// <inheritdoc/>
			public override Object Clone() {
				RangeSelectNode node = (RangeSelectNode)base.Clone();
				node.expression = (Expression)expression.Clone();
				return node;
			}

			/// <inheritdoc/>
			public override string Title {
				get { return "RANGE: " + expression; }
			}
		}

		/// <summary>
		/// The node for performing a simple select operation on a table.
		/// </summary>
		/// <remarks>
		/// The simple select requires a LHS variable, an operator, and an expression 
		/// representing the RHS.
		/// </remarks>
		[Serializable]
		public class SimpleSelectNode : SingleQueryPlanNode {
			/// <summary>
			/// The LHS variable.
			/// </summary>
			private VariableName left_var;

			/// <summary>
			/// The operator to select under (=, &lt;&gt;, &gt;, &lt;, &gt;=, &lt;=).
			/// </summary>
			private Operator op;

			/// <summary>
			/// The RHS expression.
			/// </summary>
			private Expression right_expression;

			public SimpleSelectNode(IQueryPlanNode child, VariableName left_var, Operator op, Expression right_expression)
				: base(child) {
				this.left_var = left_var;
				this.op = op;
				this.right_expression = right_expression;
			}

			public override Table Evaluate(IQueryContext context) {
				// Solve the child branch result
				Table table = child.Evaluate(context);

				// The select operation.
				return table.SimpleSelect(context,
										  left_var, op, right_expression);
			}

			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return right_expression.DiscoverTableNames(base.DiscoverTableNames(list));
			}

			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return right_expression.DiscoverCorrelatedVariables(ref level,
						 base.DiscoverCorrelatedVariables(level, list));
			}

			public override Object Clone() {
				SimpleSelectNode node = (SimpleSelectNode)base.Clone();
				node.left_var = (VariableName)left_var.Clone();
				node.right_expression = (Expression)right_expression.Clone();
				return node;
			}

			public override string Title {
				get { return "SIMPLE: " + left_var + op + right_expression; }
			}
		}

		/// <summary>
		/// The node for performing an equi-select on a group of columns of the child node.
		/// </summary>
		/// <remarks>
		/// This is a separate node instead of chained IndexedSelectNode's so that we 
		/// might exploit multi-column indexes.
		/// </remarks>
		[Serializable]
		public class MultiColumnEquiSelectNode : SingleQueryPlanNode {
			/// <summary>
			/// The list of columns to select the range of.
			/// </summary>
			private readonly VariableName[] columns;

			/// <summary>
			/// The values of the cells to equi-select (must be constant expressions).
			/// </summary>
			private readonly Expression[] values;

			public MultiColumnEquiSelectNode(IQueryPlanNode child,
											 VariableName[] columns, Expression[] values)
				: base(child) {
				this.columns = columns;
				this.values = values;
			}

			public override Table Evaluate(IQueryContext context) {
				Table t = child.Evaluate(context);

				// PENDING: Exploit multi-column indexes when they are implemented...

				// We select each column in turn
				Operator EQUALS_OP = Operator.Get("=");
				for (int i = 0; i < columns.Length; ++i) {
					t = t.SimpleSelect(context, columns[i], EQUALS_OP, values[i]);
				}

				return t;
			}

			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				throw new NotImplementedException();
			}

			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				throw new NotImplementedException();
			}

			public override Object Clone() {
				MultiColumnEquiSelectNode node =
										   (MultiColumnEquiSelectNode)base.Clone();
				CloneArray(node.columns);
				CloneArray(node.values);
				return node;
			}

		}

		/// <summary>
		/// The node for performing a functional select operation on the child node.
		/// </summary>
		/// <example>
		/// Some examples of this type of query are:
		/// <code>
		///   CONCAT(a, ' ', b) &gt; 'abba boh'
		///   TONUMBER(DATEFORMAT(a, 'yyyy')) &gt; 2001
		///   LOWER(a) &lt; 'ook'
		/// </code>
		/// The reason this is a separate node is because it is possible to exploit
		/// a functional indexes on a table with this node.
		/// <para>
		/// The given expression MUST be of the form:
		/// <code>
		///   'function_expression' 'operator' 'constant'
		/// </code>
		/// </para>
		/// </example>
		[Serializable]
		public class FunctionalSelectNode : SingleQueryPlanNode {
			/// <summary>
			/// The function expression (eg. CONCAT(a, ' ', b) == 'abba bo').
			/// </summary>
			private Expression expression;

			public FunctionalSelectNode(IQueryPlanNode child, Expression exp)
				: base(child) {
				expression = exp;
			}

			public override Table Evaluate(IQueryContext context) {
				Table t = child.Evaluate(context);
				// NOTE: currently this uses exhaustive select but should exploit
				//   function indexes when they are available.
				return t.ExhaustiveSelect(context, expression);
			}

			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return expression.DiscoverTableNames(base.DiscoverTableNames(list));
			}

			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return expression.DiscoverCorrelatedVariables(ref level,
						 base.DiscoverCorrelatedVariables(level, list));
			}

			public override Object Clone() {
				FunctionalSelectNode node = (FunctionalSelectNode)base.Clone();
				node.expression = (Expression)expression.Clone();
				return node;
			}

		}

		/// <summary>
		/// The node for performing a exhaustive select operation on 
		/// the child node.
		/// </summary>
		/// <remarks>
		/// This node will iterate through the entire child result and all
		/// results that evaulate to true are included in the result.
		/// <para>
		/// <b>Note:</b> The expression may have correlated sub-queries.
		/// </para>
		/// </remarks>
		[Serializable]
		public class ExhaustiveSelectNode : SingleQueryPlanNode {
			/// <summary>
			/// The search expression.
			/// </summary>
			private Expression expression;

			public ExhaustiveSelectNode(IQueryPlanNode child, Expression exp)
				:base(child) {
				expression = exp;
			}

			public override Table Evaluate(IQueryContext context) {
				Table t = child.Evaluate(context);
				return t.ExhaustiveSelect(context, expression);
			}

			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return expression.DiscoverTableNames(base.DiscoverTableNames(list));
			}

			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return expression.DiscoverCorrelatedVariables(ref level,
						 base.DiscoverCorrelatedVariables(level, list));
			}

			public override Object Clone() {
				ExhaustiveSelectNode node = (ExhaustiveSelectNode)base.Clone();
				node.expression = (Expression)expression.Clone();
				return node;
			}

			public override string Title {
				get { return "EXHAUSTIVE: " + expression; }
			}
		}

		/// <summary>
		/// The node for evaluating an expression that contains entirely 
		/// constant values (no variables).
		/// </summary>
		[Serializable]
		public class ConstantSelectNode : SingleQueryPlanNode {
			/// <summary>
			/// The search expression.
			/// </summary>
			private Expression expression;

			public ConstantSelectNode(IQueryPlanNode child, Expression exp)
				: base(child) {
				expression = exp;
			}

			public override Table Evaluate(IQueryContext context) {
				// Evaluate the expression
				TObject v = expression.Evaluate(null, null, context);
				// If it evaluates to NULL or FALSE then return an empty set
				if (v.IsNull || v.Object.Equals(false))
					return child.Evaluate(context).EmptySelect();
				return child.Evaluate(context);
			}

			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return expression.DiscoverTableNames(base.DiscoverTableNames(list));
			}

			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return expression.DiscoverCorrelatedVariables(ref level,
						 base.DiscoverCorrelatedVariables(level, list));
			}

			public override Object Clone() {
				ConstantSelectNode node = (ConstantSelectNode)base.Clone();
				node.expression = (Expression)expression.Clone();
				return node;
			}

			public override string Title {
				get { return "CONSTANT: " + expression; }
			}
		}

		/// <summary>
		/// The node for evaluating a simple pattern search on a table which
		/// includes a single left hand variable or constant, a pattern type 
		/// (<i>LIKE</i>, <i>NOT LIKE</i> or <i>REGEXP</i>), and a right hand 
		/// constant (eg. <c>T__y</c>).
		/// </summary>
		/// <remarks>
		/// If the expression is not in the form described above then this 
		/// node will not operate correctly.
		/// </remarks>
		[Serializable]
		public class SimplePatternSelectNode : SingleQueryPlanNode {
			/// <summary>
			/// The search expression.
			/// </summary>
			private Expression expression;

			public SimplePatternSelectNode(IQueryPlanNode child, Expression exp)
				: base(child) {
				expression = exp;
			}

			public override Table Evaluate(IQueryContext context) {
				// Evaluate the child
				Table t = child.Evaluate(context);
				// Perform the pattern search expression on the table.
				// Split the expression,
				Expression[] exps = expression.Split();
				VariableName lhs_var = exps[0].VariableName;
				if (lhs_var != null) {
					// LHS is a simple variable so do a simple select
					Operator op = (Operator) expression.Last;
					return t.SimpleSelect(context, lhs_var, op, exps[1]);
				}
				// LHS must be a constant so we can just evaluate the expression
				// and see if we get true, false, null, etc.
				TObject v = expression.Evaluate(null, context);
				// If it evaluates to NULL or FALSE then return an empty set
				if (v.IsNull || v.Object.Equals(false))
					return t.EmptySelect();
				return t;
			}

			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return expression.DiscoverTableNames(base.DiscoverTableNames(list));
			}

			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return expression.DiscoverCorrelatedVariables(ref level,
						 base.DiscoverCorrelatedVariables(level, list));
			}

			public override Object Clone() {
				SimplePatternSelectNode node = (SimplePatternSelectNode)base.Clone();
				node.expression = (Expression)expression.Clone();
				return node;
			}

			public override string Title {
				get { return "PATTERN: " + expression; }
			}
		}

		/// <summary>
		/// The node for finding a subset and renaming the columns of the 
		/// results in the child node.
		/// </summary>
		[Serializable]
		public class SubsetNode : SingleQueryPlanNode {
			/// <summary>
			/// The original columns in the child that we are to make the subset of.
			/// </summary>
			private readonly VariableName[] original_columns;

			/// <summary>
			/// New names to assign the columns.
			/// </summary>
			private readonly VariableName[] new_column_names;


			public SubsetNode(IQueryPlanNode child,
							  VariableName[] original_columns,
							  VariableName[] new_column_names)
				: base(child) {
				this.original_columns = original_columns;
				this.new_column_names = new_column_names;

			}

			public override Table Evaluate(IQueryContext context) {
				Table t = child.Evaluate(context);

				int sz = original_columns.Length;
				int[] col_map = new int[sz];

				for (int i = 0; i < sz; ++i) {
					col_map[i] = t.FindFieldName(original_columns[i]);
				}

				SubsetColumnTable col_table = new SubsetColumnTable(t);
				col_table.SetColumnMap(col_map, new_column_names);

				return col_table;
			}

			// ---------- Set methods ----------

			/// <summary>
			/// Sets the given table name of the resultant table.
			/// </summary>
			/// <param name="name"></param>
			/// <remarks>
			/// This is intended if we want to create a sub-query that has an 
			/// aliased table name.
			/// </remarks>
			public void SetGivenName(TableName name) {
				//      given_name = name;
				if (name != null) {
					int sz = new_column_names.Length;
					for (int i = 0; i < sz; ++i) {
						new_column_names[i].TableName = name;
					}
				}
			}

			// ---------- Get methods ----------

			/// <summary>
			/// Returns the list of original columns that represent the mappings from
			/// the columns in this subset.
			/// </summary>
			public VariableName[] OriginalColumns {
				get { return original_columns; }
			}

			/// <summary>
			/// Returns the list of new column names that represent the new 
			/// columns in this subset.
			/// </summary>
			public VariableName[] NewColumnNames {
				get { return new_column_names; }
			}

			public override Object Clone() {
				SubsetNode node = (SubsetNode)base.Clone();
				CloneArray(node.original_columns);
				CloneArray(node.new_column_names);
				return node;
			}

			public override string Title {
				get {
					StringBuilder buf = new StringBuilder();
					buf.Append("SUBSET: ");
					for (int i = 0; i < new_column_names.Length; ++i) {
						buf.Append(new_column_names[i]);
						buf.Append("->");
						buf.Append(original_columns[i]);
						buf.Append(", ");
					}
					return buf.ToString();
				}
			}
		}

		/// <summary>
		/// The node for performing a distinct operation on the given 
		/// columns of the child node.
		/// </summary>
		[Serializable]
		public class DistinctNode : SingleQueryPlanNode {
			/// <summary>
			/// The list of columns to be distinct.
			/// </summary>
			private readonly VariableName[] columns;

			public DistinctNode(IQueryPlanNode child, VariableName[] columns)
				: base(child) {
				this.columns = columns;
			}

			public override Table Evaluate(IQueryContext context) {
				Table t = child.Evaluate(context);
				int sz = columns.Length;
				int[] col_map = new int[sz];
				for (int i = 0; i < sz; ++i) {
					col_map[i] = t.FindFieldName(columns[i]);
				}
				return t.Distinct(col_map);
			}

			public override Object Clone() {
				DistinctNode node = (DistinctNode)base.Clone();
				CloneArray(node.columns);
				return node;
			}

			public override string Title {
				get {
					StringBuilder buf = new StringBuilder();
					buf.Append("DISTINCT: (");
					for (int i = 0; i < columns.Length; ++i) {
						buf.Append(columns[i]);
						buf.Append(", ");
					}
					buf.Append(")");
					return buf.ToString();
				}
			}
		}

		/// <summary>
		/// The node for performing a sort operation on the given columns of 
		/// the child node.
		/// </summary>
		[Serializable]
		public class SortNode : SingleQueryPlanNode {
			/// <summary>
			/// The list of columns to sort.
			/// </summary>
			private readonly VariableName[] columns;

			/// <summary>
			/// Whether to sort the column in ascending or descending order
			/// </summary>
			private readonly bool[] correct_ascending;

			public SortNode(IQueryPlanNode child, VariableName[] columns, bool[] ascending)
				: base(child) {
				this.columns = columns;
				correct_ascending = ascending;

				// How we handle ascending/descending order
				// ----------------------------------------
				// Internally to the database, all columns are naturally ordered in
				// ascending order (start at lowest and end on highest).  When a column
				// is ordered in descending order, a fast way to achieve this is to take
				// the ascending set and reverse it.  This works for single columns,
				// however some thought is required for handling multiple column.  We
				// order columns from RHS to LHS.  If LHS is descending then this will
				// order the RHS incorrectly if we leave as is.  Therefore, we must do
				// some pre-processing that looks ahead on any descending orders and
				// reverses the order of the columns to the right.  This pre-processing
				// is done in the first pass.

				int sz = ascending.Length;
				for (int n = 0; n < sz - 1; ++n) {
					if (!ascending[n]) {    // if descending...
						// Reverse order of all columns to the right...
						for (int p = n + 1; p < sz; ++p) {
							ascending[p] = !ascending[p];
						}
					}
				}

			}

			public override Table Evaluate(IQueryContext context) {
				Table t = child.Evaluate(context);
				// Sort the results by the columns in reverse-safe order.
				int sz = correct_ascending.Length;
				for (int n = sz - 1; n >= 0; --n) {
					t = t.OrderByColumn(columns[n], correct_ascending[n]);
				}
				return t;
			}

			public override Object Clone() {
				SortNode node = (SortNode)base.Clone();
				CloneArray(node.columns);
				return node;
			}

			public override string Title {
				get {
					StringBuilder buf = new StringBuilder();
					buf.Append("SORT: (");
					for (int i = 0; i < columns.Length; ++i) {
						buf.Append(columns[i]);
						if (correct_ascending[i]) {
							buf.Append(" ASC");
						} else {
							buf.Append(" DESC");
						}
						buf.Append(", ");
					}
					buf.Append(")");
					return buf.ToString();
				}
			}
		}

		/// <summary>
		/// The node for performing a grouping operation on the columns of the 
		/// child node.
		/// </summary>
		/// <remarks>
		/// As well as grouping, any aggregate functions must also be defined
		/// with this plan.
		/// <para>
		/// <b>Note:</b> The whole child is a group if columns is null.
		/// </para>
		/// </remarks>
		[Serializable]
		public class GroupNode : SingleQueryPlanNode {
			/// <summary>
			/// The columns to group by.
			/// </summary>
			private readonly VariableName[] columns;

			/// <summary>
			/// The group max column.
			/// </summary>
			private VariableName group_max_column;

			/// <summary>
			/// Any aggregate functions (or regular function columns) that 
			/// are to be planned.
			/// </summary>
			private readonly Expression[] function_list;

			/// <summary>
			/// The list of names to give each function table.
			/// </summary>
			private readonly String[] name_list;


			/// <summary>
			/// Groups over the given columns from the child.
			/// </summary>
			/// <param name="child"></param>
			/// <param name="columns"></param>
			/// <param name="group_max_column"></param>
			/// <param name="function_list"></param>
			/// <param name="name_list"></param>
			public GroupNode(IQueryPlanNode child, VariableName[] columns,
							 VariableName group_max_column,
							 Expression[] function_list, String[] name_list)
				: base(child) {
				this.columns = columns;
				this.group_max_column = group_max_column;
				this.function_list = function_list;
				this.name_list = name_list;
			}

			/// <summary>
			/// Groups over the entire child (always ends in 1 result in set).
			/// </summary>
			/// <param name="child"></param>
			/// <param name="group_max_column"></param>
			/// <param name="function_list"></param>
			/// <param name="name_list"></param>
			public GroupNode(IQueryPlanNode child, VariableName group_max_column,
							 Expression[] function_list, String[] name_list)
				: this(child, null, group_max_column, function_list, name_list) {
			}

			public override Table Evaluate(IQueryContext context) {
				Table child_table = child.Evaluate(context);
				DatabaseQueryContext db_context = (DatabaseQueryContext)context;
				FunctionTable fun_table =
				   new FunctionTable(child_table, function_list, name_list, db_context);
				// If no columns then it is implied the whole table is the group.
				if (columns == null) {
					fun_table.SetWholeTableAsGroup();
				} else {
					fun_table.CreateGroupMatrix(columns);
				}
				return fun_table.MergeWithReference(group_max_column);
			}

			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				list = base.DiscoverTableNames(list);
				for (int i = 0; i < function_list.Length; ++i) {
					list = function_list[i].DiscoverTableNames(list);
				}
				return list;
			}

			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				list = base.DiscoverCorrelatedVariables(level, list);
				for (int i = 0; i < function_list.Length; ++i) {
					list = function_list[i].DiscoverCorrelatedVariables(ref level, list);
				}
				return list;
			}

			public override Object Clone() {
				GroupNode node = (GroupNode)base.Clone();
				CloneArray(node.columns);
				CloneArray(node.function_list);
				if (group_max_column != null) {
					node.group_max_column = (VariableName)group_max_column.Clone();
				} else {
					node.group_max_column = null;
				}
				return node;
			}

			public override string Title {
				get {
					StringBuilder buf = new StringBuilder();
					buf.Append("GROUP: (");
					if (columns == null) {
						buf.Append("WHOLE TABLE");
					} else {
						for (int i = 0; i < columns.Length; ++i) {
							buf.Append(columns[i]);
							buf.Append(", ");
						}
					}
					buf.Append(")");
					if (function_list != null) {
						buf.Append(" FUNS: [");
						for (int i = 0; i < function_list.Length; ++i) {
							buf.Append(function_list[i]);
							buf.Append(", ");
						}
						buf.Append("]");
					}
					return buf.ToString();
				}
			}
		}

		/// <summary>
		/// The node for merging the child node with a set of new function 
		/// columns over the entire result.
		/// </summary>
		/// <remarks>
		/// For example, we may want to add an expression <c>a + 10</c> or 
		/// <c>coalesce(a, b, 1)</c>.
		/// </remarks>
		[Serializable]
		public class CreateFunctionsNode : SingleQueryPlanNode {
			/// <summary>
			/// The list of functions to create.
			/// </summary>
			private readonly Expression[] function_list;

			/// <summary>
			/// The list of names to give each function table.
			/// </summary>
			private readonly String[] name_list;

			public CreateFunctionsNode(IQueryPlanNode child, Expression[] function_list,
									   String[] name_list)
				: base(child) {
				this.function_list = function_list;
				this.name_list = name_list;
			}

			public override Table Evaluate(IQueryContext context) {
				Table child_table = child.Evaluate(context);
				DatabaseQueryContext db_context = (DatabaseQueryContext)context;
				FunctionTable fun_table =
					new FunctionTable(child_table, function_list, name_list, db_context);
				Table t = fun_table.MergeWithReference(null);
				return t;
			}

			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				list = base.DiscoverTableNames(list);
				for (int i = 0; i < function_list.Length; ++i) {
					list = function_list[i].DiscoverTableNames(list);
				}
				return list;
			}

			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				list = base.DiscoverCorrelatedVariables(level, list);
				for (int i = 0; i < function_list.Length; ++i) {
					list = function_list[i].DiscoverCorrelatedVariables(ref level, list);
				}
				return list;
			}

			public override Object Clone() {
				CreateFunctionsNode node = (CreateFunctionsNode)base.Clone();
				CloneArray(node.function_list);
				return node;
			}

			public override string Title {
				get {
					StringBuilder buf = new StringBuilder();
					buf.Append("FUNCTIONS: (");
					for (int i = 0; i < function_list.Length; ++i) {
						buf.Append(function_list[i]);
						buf.Append(", ");
					}
					buf.Append(")");
					return buf.ToString();
				}
			}
		}

		/// <summary>
		/// A marker node that takes the result of a child and marks it as 
		/// a name that can later be retrieved.
		/// </summary>
		/// <remarks>
		/// This is useful for implementing things such as outer joins.
		/// </remarks>
		[Serializable]
		public class MarkerNode : SingleQueryPlanNode {
			/// <summary>
			/// The name of this mark.
			/// </summary>
			private readonly String mark_name;

			public MarkerNode(IQueryPlanNode child, String mark_name)
				: base(child) {
				this.mark_name = mark_name;
			}

			public override Table Evaluate(IQueryContext context) {
				Table child_table = child.Evaluate(context);
				context.AddMarkedTable(mark_name, child_table);
				return child_table;
			}

			public override Object Clone() {
				return base.Clone();
			}

		}

		/// <summary>
		/// A node that only evaluates the child if the result can not be 
		/// found in the cache with the given unique id.
		/// </summary>
		[Serializable]
		public class CachePointNode : SingleQueryPlanNode {
			/// <summary>
			/// The unique identifier of this cache point.
			/// </summary>
			private readonly long id;

			private readonly static Object GLOB_LOCK = new Object();
			private static int GLOB_ID;

			public CachePointNode(IQueryPlanNode child)
				: base(child) {
				lock (GLOB_LOCK) {
					id = (DateTime.Now.Ticks << 16) | (GLOB_ID & 0x0FFFF);
					++GLOB_ID;
				}
			}

			public override Table Evaluate(IQueryContext context) {
				// Is the result available in the context?
				Table child_table = context.GetCachedNode(id);
				if (child_table == null) {
					// No so evaluate the child and cache it
					child_table = child.Evaluate(context);
					context.PutCachedNode(id, child_table);
				}
				return child_table;
			}

			public override Object Clone() {
				return base.Clone();
			}

			public override string Title {
				get { return "CACHE: " + id; }
			}
		}

		/// <summary>
		/// A branch node for naturally joining two tables together.
		/// </summary>
		/// <remarks>
		/// These branches should be optimized out if possible because they 
		/// result in huge results.
		/// </remarks>
		[Serializable]
		public class NaturalJoinNode : BranchQueryPlanNode {
			public NaturalJoinNode(IQueryPlanNode left, IQueryPlanNode right)
				: base(left, right) {
			}

			public override Table Evaluate(IQueryContext context) {
				// Solve the left branch result
				Table left_result = left.Evaluate(context);
				// Solve the Join (natural)
				return left_result.Join(right.Evaluate(context));
			}

			public override string Title {
				get { return "NATURAL JOIN"; }
			}
		}

		/// <summary>
		/// A branch node for equi-joining two tables together given two 
		/// sets of columns.
		/// </summary>
		/// <remarks>
		/// This is a seperate node from a general join operation to allow
		/// for optimizations with multi-column indexes.
		/// <para>
		/// An equi-join is the most common type of join.
		/// </para>
		/// <para>
		/// At query runtime, this decides the best best way to perform the 
		/// join, either by
		/// </para>
		/// </remarks>
		[Serializable]
		public class EquiJoinNode : BranchQueryPlanNode {
			/// <summary>
			/// The columns in the left table.
			/// </summary>
			private readonly VariableName[] left_columns;

			/// <summary>
			/// The columns in the right table.
			/// </summary>
			private readonly VariableName[] right_columns;

			public EquiJoinNode(IQueryPlanNode left, IQueryPlanNode right,
								VariableName[] left_cols, VariableName[] right_cols)
				: base(left, right) {
				left_columns = left_cols;
				right_columns = right_cols;
			}

			public override Table Evaluate(IQueryContext context) {
				// Solve the left branch result
				Table left_result = left.Evaluate(context);
				// Solve the right branch result
				Table right_result = right.Evaluate(context);

				// PENDING: This needs to migrate to a better implementation that
				//   exploits multi-column indexes if one is defined that can be used.

				VariableName first_left = left_columns[0];
				VariableName first_right = right_columns[0];

				Operator EQUALS_OP = Operator.Get("=");

				Table result = left_result.SimpleJoin(context, right_result,
					 first_left, EQUALS_OP, new Expression(first_right));

				int sz = left_columns.Length;
				// If there are columns left to equi-join, we resolve the rest with a
				// single exhaustive select of the form,
				//   ( table1.col2 = table2.col2 AND table1.col3 = table2.col3 AND ... )
				if (sz > 1) {
					// Form the expression
					Expression rest_expression = new Expression();
					for (int i = 1; i < sz; ++i) {
						VariableName left_var = left_columns[i];
						VariableName right_var = right_columns[i];
						rest_expression.AddElement(left_var);
						rest_expression.AddElement(right_var);
						rest_expression.AddOperator(EQUALS_OP);
					}
					Operator AND_OP = Operator.Get("and");
					for (int i = 2; i < sz; ++i) {
						rest_expression.AddOperator(AND_OP);
					}
					result = result.ExhaustiveSelect(context, rest_expression);
				}

				return result;
			}

			public override Object Clone() {
				EquiJoinNode node = (EquiJoinNode)base.Clone();
				CloneArray(node.left_columns);
				CloneArray(node.right_columns);
				return node;
			}

		}

		/// <summary>
		/// A branch node for a non-equi join between two tables.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> The cost of a LeftJoin is higher if the right child 
		/// result is greater than the left child result. The plan should be 
		/// arranged so smaller results are on the left.
		/// </remarks>
		[Serializable]
		public class JoinNode : BranchQueryPlanNode {
			/// <summary>
			/// The variable in the left table to be joined.
			/// </summary>
			private VariableName left_var;

			/// <summary>
			/// The operator to join under (=, &lt;&gt;, &gt;, &lt;, &gt;=, &lt;=).
			/// </summary>
			private readonly Operator join_op;

			/// <summary>
			/// The expression evaluated on the right table.
			/// </summary>
			private Expression right_expression;

			public JoinNode(IQueryPlanNode left, IQueryPlanNode right,
							VariableName left_var, Operator join_op,
							Expression right_expression)
				: base(left, right) {
				this.left_var = left_var;
				this.join_op = join_op;
				this.right_expression = right_expression;
			}

			public override Table Evaluate(IQueryContext context) {
				// Solve the left branch result
				Table left_result = left.Evaluate(context);
				// Solve the right branch result
				Table right_result = right.Evaluate(context);

				// If the right_expression is a simple variable then we have the option
				// of optimizing this join by putting the smallest table on the LHS.
				VariableName rhs_var = right_expression.VariableName;
				VariableName lhs_var = left_var;
				Operator op = join_op;
				if (rhs_var != null) {
					// We should arrange the expression so the right table is the smallest
					// of the sides.
					// If the left result is less than the right result
					if (left_result.RowCount < right_result.RowCount) {
						// Reverse the join
						right_expression = new Expression(lhs_var);
						lhs_var = rhs_var;
						op = op.Reverse();
						// Reverse the tables.
						Table t = right_result;
						right_result = left_result;
						left_result = t;
					}
				}

				// The join operation.
				return left_result.SimpleJoin(context, right_result,
											  lhs_var, op, right_expression);
			}

			public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
				return right_expression.DiscoverTableNames(base.DiscoverTableNames(list));
			}

			public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
				return right_expression.DiscoverCorrelatedVariables(ref level,
						 base.DiscoverCorrelatedVariables(level, list));
			}

			public override Object Clone() {
				JoinNode node = (JoinNode)base.Clone();
				node.left_var = (VariableName)left_var.Clone();
				node.right_expression = (Expression)right_expression.Clone();
				return node;
			}

			public override string Title {
				get {
					return "JOIN: " + left_var + join_op +
					       right_expression;
				}
			}
		}

		/// <summary>
		/// A branch node for a left outer join.
		/// </summary>
		/// <remarks>
		/// Using this node is a little non-intuitive. This node will only 
		/// work when used in conjuction with <see cref="MarkerNode"/>.
		/// <para>
		/// To use - first the complete left table in the join must be marked 
		/// with a name. Then the <i>on expression</i> is evaluated to a single 
		/// plan node. Then this plan node must be added to result in a left 
		/// outer join. 
		/// A tree for a left outer join may look as follows:
		/// <code>
		///          LeftOuterJoinNode
		///                  |
		///              Join a = b
		///              /         \
		///          Marker      GetTable T2
		///            |
		///       GetTable T1
		/// </code>
		/// </para>
		/// </remarks>
		[Serializable]
		public class LeftOuterJoinNode : SingleQueryPlanNode {
			/// <summary>
			/// The name of the mark that points to the left table that represents
			/// the complete set.
			/// </summary>
			private readonly string complete_mark_name;

			public LeftOuterJoinNode(IQueryPlanNode child, String complete_mark_name)
				: base(child) {
				this.complete_mark_name = complete_mark_name;
			}

			public override Table Evaluate(IQueryContext context) {
				// Evaluate the child branch,
				Table result = child.Evaluate(context);
				// Get the table of the complete mark name,
				Table complete_left = context.GetMarkedTable(complete_mark_name);

				// The rows in 'complete_left' that are outside (not in) the rows in the
				// left result.
				Table outside = complete_left.Outside(result);

				// Create an OuterTable
				OuterTable outer_table = new OuterTable(result);
				outer_table.MergeIn(outside);

				// Return the outer table
				return outer_table;
			}

			public override string Title {
				get { return "LEFT OUTER JOIN"; }
			}
		}

		/// <summary>
		/// A branch node for a logical union of two tables of identical types.
		/// </summary>
		/// <remarks>
		/// This branch can only work if the left and right children have 
		/// exactly the same ancestor tables. If the ancestor tables are 
		/// different it will fail. This node is used for logical <b>or</b>.
		/// <para>
		/// This union does not include duplicated rows.
		/// </para>
		/// </remarks>
		[Serializable]
		public class LogicalUnionNode : BranchQueryPlanNode {
			public LogicalUnionNode(IQueryPlanNode left, IQueryPlanNode right)
				: base(left, right) {
			}

			public override Table Evaluate(IQueryContext context) {
				// Solve the left branch result
				Table left_result = left.Evaluate(context);
				// Solve the right branch result
				Table right_result = right.Evaluate(context);

				return left_result.Union(right_result);
			}

			public override string Title {
				get { return "LOGICAL Union"; }
			}
		}

		/// <summary>
		/// A branch node for performing a composite function on two child nodes.
		/// </summary>
		/// <remarks>
		/// This branch is used for general <see cref="CompositeFunction.Union"/>, 
		/// <see cref="CompositeFunction.Except"/>, <see cref="CompositeFunction.Intersect"/>
		/// composites. The left and right branch results must have the same number of 
		/// columns and column types.
		/// </remarks>
		[Serializable]
		public class CompositeNode : BranchQueryPlanNode {
			/// <summary>
			/// The composite operation.
			/// </summary>
			private readonly CompositeFunction composite_op;

			/// <summary>
			/// If this is true, the composite includes all results from both 
			/// children, otherwise removes deplicates.
			/// </summary>
			private readonly bool all_op;

			public CompositeNode(IQueryPlanNode left, IQueryPlanNode right,
								 CompositeFunction composite_op, bool all_op)
				: base(left, right) {
				this.composite_op = composite_op;
				this.all_op = all_op;
			}

			public override Table Evaluate(IQueryContext context) {
				// Solve the left branch result
				Table left_result = left.Evaluate(context);
				// Solve the right branch result
				Table right_result = right.Evaluate(context);

				// Form the composite table
				CompositeTable t = new CompositeTable(left_result,
										   new Table[] { left_result, right_result });
				t.SetupIndexesForCompositeFunction(composite_op, all_op);

				return t;
			}

		}

		/// <summary>
		/// A branch node for a non-correlated ANY or ALL sub-query evaluation.
		/// </summary>
		/// <remarks>
		/// This node requires a set of columns from the left branch and an 
		/// operator.
		/// The right branch represents the non-correlated sub-query.
		/// <para>
		/// <b>Note:</b> The cost of a SubQuery is higher if the right child 
		/// result is greater than the left child result. The plan should be 
		/// arranged so smaller results are on the left.
		/// </para>
		/// </remarks>
		[Serializable]
		public class NonCorrelatedAnyAllNode : BranchQueryPlanNode {
			/// <summary>
			/// The columns in the left table.
			/// </summary>
			private readonly VariableName[] left_columns;

			/// <summary>
			/// The SubQuery operator, eg. '= ANY', '&lt;&gt; ALL'
			/// </summary>
			private readonly Operator sub_query_operator;

			public NonCorrelatedAnyAllNode(IQueryPlanNode left, IQueryPlanNode right,
									  VariableName[] left_vars, Operator subquery_op)
				: base(left, right) {
				this.left_columns = left_vars;
				this.sub_query_operator = subquery_op;
			}

			public override Table Evaluate(IQueryContext context) {
				// Solve the left branch result
				Table left_result = left.Evaluate(context);
				// Solve the right branch result
				Table right_result = right.Evaluate(context);

				// Solve the sub query on the left columns with the right plan and the
				// given operator.
				return TableFunctions.AnyAllNonCorrelated(left_result, left_columns,
													sub_query_operator, right_result);
			}

			public override Object Clone() {
				NonCorrelatedAnyAllNode node = (NonCorrelatedAnyAllNode)base.Clone();
				CloneArray(node.left_columns);
				return node;
			}

			public override string Title {
				get {
					StringBuilder buf = new StringBuilder();
					buf.Append("NON_CORRELATED: (");
					for (int i = 0; i < left_columns.Length; ++i) {
						buf.Append(left_columns[i].ToString());
					}
					buf.Append(") ");
					buf.Append(sub_query_operator.ToString());
					return buf.ToString();
				}
			}
		}
	}
}