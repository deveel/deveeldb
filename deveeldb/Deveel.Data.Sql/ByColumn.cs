//  
//  ByColumn.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
		public Object Clone() {
			ByColumn v = (ByColumn)MemberwiseClone();
			if (exp != null)
				v.exp = (Expression)exp.Clone();
			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			return "ByColumn(" + exp + ", " + ascending + ")";
		}

	}
}