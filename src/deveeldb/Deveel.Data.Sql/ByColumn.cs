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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Object used to represent a column in the <i>ORDER BY</i> and 
	/// <i>GROUP BY</i> clauses of a select statement.
	/// </summary>
	[Serializable]
	public sealed class ByColumn : IStatementTreeObject {
		/// <summary>
		/// Constructs the <c>BY</c> column reference with the expression
		/// and the sort order given.
		/// </summary>
		/// <param name="exp">The expression of the column reference.</param>
		/// <param name="ascending">The sort order for the column. If this is
		/// set to <b>true</b>, the column will be used to sort the results of
		/// a query in ascending order.</param>
		public ByColumn(Expression exp, bool ascending) {
			this.exp = exp;
			this.ascending = ascending;
		}

		/// <summary>
		/// Constructs the <c>BY</c> column reference with the expression
		/// given and the ascending sort order.
		/// </summary>
		/// <param name="exp">The expression of the column reference.</param>
		public ByColumn(Expression exp)
			: this(exp, true) {
		}

		/// <summary>
		/// Constructs the <c>BY</c> column reference with the expression
		/// and the sort order given.
		/// </summary>
		/// <param name="exp">The expression of the column reference.</param>
		/// <param name="ascending">The sort order for the column. If this is
		/// set to <b>true</b>, the column will be used to sort the results of
		/// a query in ascending order.</param>
		public ByColumn(string exp, bool ascending)
			: this(Expression.Parse(exp), ascending) {
		}

		/// <summary>
		/// Constructs the <c>BY</c> column reference with the expression
		/// given and the ascending sort order.
		/// </summary>
		/// <param name="exp">The expression of the column reference.</param>
		public ByColumn(string exp)
			: this(exp, true) {
		}

		/// <summary>
		/// The expression that we are ordering by.
		/// </summary>
		private Expression exp;

		/// <summary>
		/// If 'order by' then true if sort is ascending (default).
		/// </summary>
		private readonly bool ascending = true;

		/// <summary>
		/// Gets the expression used to order the result of a query.
		/// </summary>
		public Expression Expression {
			get { return exp; }
		}

		/// <summary>
		/// Gets a boolean value indicating whether we're sorting in ascending
		/// or descending order.
		/// </summary>
		public bool Ascending {
			get { return ascending; }
		}

		internal void SetExpression(Expression expression) {
			exp = expression;
		}

		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			if (exp != null) {
				exp.Prepare(preparer);
			}
		}

		/// <inheritdoc/>
		public object Clone() {
			ByColumn v = (ByColumn)MemberwiseClone();
			if (exp != null)
				v.exp = (Expression)exp.Clone();
			return v;
		}
	}
}