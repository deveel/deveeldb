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

using Deveel.Data;
using Deveel.Data.Index;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
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
	class RangeSelectNode : SingleQueryPlanNode {
		public RangeSelectNode(IQueryPlanNode child, SqlExpression expression)
			: base(child) {
			Expression = expression;
		}

		/// <summary>
		/// A simple expression that represents the range to select.  See the
		/// class comments for a description for how this expression must be
		/// formed.
		/// </summary>
		public SqlExpression Expression { get; private set; }

		/// <inheritdoc/>
		public override ITable Evaluate(IQuery context) {
			var t = Child.Evaluate(context);

			var exp = Expression;

			// Assert that all variables in the expression are identical.
			var columnNames = exp.DiscoverReferences();
			ObjectName columnName = null;
			foreach (var cv in columnNames) {
				if (columnName != null && !cv.Equals(columnName))
					throw new InvalidOperationException("Range plan does not contain common column.");

				columnName = cv;
			}

			// Find the variable field in the table.
			var col = t.IndexOfColumn(columnName);
			if (col == -1)
				throw new InvalidOperationException("Could not find column reference in table: " + columnName);

			var field = t.TableInfo[col];

			// Calculate the range
			var range = new IndexRangeSet();
			var calculator = new RangeSetCalculator(context, field, range);
			range = calculator.Calculate(exp);

			// Select the range from the table
			var ranges = range.ToArray();
			return t.SelectRange(columnName, ranges);
		}

		#region RangeSetUpdater

		class RangeSetUpdater : SqlExpressionVisitor {
			private IndexRangeSet indexRangeSet;
			private readonly IQuery context;
			private readonly ColumnInfo field;

			public RangeSetUpdater(IQuery context, ColumnInfo field, IndexRangeSet indexRangeSet) {
				this.context = context;
				this.field = field;
				this.indexRangeSet = indexRangeSet;
			}

			public IndexRangeSet Update(SqlExpression expression) {
				Visit(expression);
				return indexRangeSet;
			}

			public override SqlExpression VisitBinary(SqlBinaryExpression binaryEpression) {
				var op = binaryEpression.ExpressionType;

				// Evaluate to an object
				var value = binaryEpression.Right.EvaluateToConstant(context, null);

				// If the evaluated object is not of a comparable type, then it becomes
				// null.
				var fieldType = field.ColumnType;
				if (!value.Type.IsComparable(fieldType))
					value = DataObject.Null(fieldType);

				// Intersect this in the range set
				indexRangeSet = indexRangeSet.Intersect(op, value);

				return base.VisitBinary(binaryEpression);
			}
		}

		#endregion

		#region RangeSetCalculator

		class RangeSetCalculator : SqlExpressionVisitor {
			private IndexRangeSet rangeSet;
			private readonly IQuery context;
			private readonly ColumnInfo field;

			public RangeSetCalculator(IQuery context, ColumnInfo field, IndexRangeSet rangeSet) {
				this.context = context;
				this.field = field;
				this.rangeSet = rangeSet;
			}

			private IndexRangeSet UpdateRange(SqlExpression expression) {
				var updater = new RangeSetUpdater(context, field, rangeSet);
				return updater.Update(expression);
			}

			private IndexRangeSet CalcExpression(SqlExpression expression) {
				var indexRangeSet = new IndexRangeSet();
				var calculator = new RangeSetCalculator(context, field, indexRangeSet);
				return calculator.Calculate(expression);
			}

			public override SqlExpression VisitBinary(SqlBinaryExpression binaryEpression) {
				if (binaryEpression.ExpressionType == SqlExpressionType.And) {
					rangeSet = UpdateRange(binaryEpression.Left);
					rangeSet = UpdateRange(binaryEpression.Right);
				} else if (binaryEpression.ExpressionType == SqlExpressionType.Or) {
					var left = CalcExpression(binaryEpression.Left);
					var right = CalcExpression(binaryEpression.Right);

					rangeSet = rangeSet.Union(left);
					rangeSet = rangeSet.Union(right);
				} else {
					rangeSet = UpdateRange(binaryEpression);
				}

				return base.VisitBinary(binaryEpression);
			}

			public IndexRangeSet Calculate(SqlExpression expression) {
				Visit(expression);
				return rangeSet;
			}
		}

		#endregion
	}
}