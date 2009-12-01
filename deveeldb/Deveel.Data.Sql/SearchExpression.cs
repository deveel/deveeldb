//  
//  SearchExpression.cs
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
using System.Text;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Search expression is a form of an <see cref="Expression"/> that is 
	/// split up into component parts that can be easily formed into a 
	/// search command.
	/// </summary>
	[Serializable]
	public sealed class SearchExpression : IStatementTreeObject {
		internal SearchExpression() {
		}

		/// <summary>
		/// Constructs a new <see cref="SearchExpression"/> that encapsulates
		/// the given <see cref="Expression"/>.
		/// </summary>
		/// <param name="expression">The <see cref="Expression"/> used to search
		/// in a <c>SELECT</c> statement.</param>
		public SearchExpression(Expression expression) {
			search_expression = expression;
		}

		/// <summary>
		/// The originating expression.
		/// </summary>
		private Expression search_expression;

		/// <summary>
		/// Gets this search expression from the given expression.
		/// </summary>
		public Expression FromExpression {
			get { return search_expression; }
		}

		internal void SetFromExpression(Expression expression) {
			search_expression = expression;
		}

		/// <summary>
		/// Concatenates a new expression to the end of this expression 
		/// and uses the <c>AND</c> operator to seperate the expressions.
		/// </summary>
		/// <param name="expression"></param>
		/// <remarks>
		/// This is very useful for adding new logical conditions to the 
		/// expression at runtime.
		/// </remarks>
		internal void AppendExpression(Expression expression) {
			search_expression = search_expression == null
			                    	? expression
			                    	: new Expression(search_expression, Operator.Get("and"), expression);
		}


		///<summary>
		/// Prepares the expression.
		///</summary>
		///<param name="preparer"></param>
		internal void Prepare(IExpressionPreparer preparer) {
			if (search_expression != null) {
				search_expression.Prepare(preparer);
			}
		}

		/// <inheritdoc/>
		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			Prepare(preparer);
		}

		/// <inheritdoc/>
		public Object Clone() {
			SearchExpression v = (SearchExpression)MemberwiseClone();
			if (search_expression != null) {
				v.search_expression = (Expression)search_expression.Clone();
			}
			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			if (search_expression != null)
				return search_expression.ToString();
			return "NO SEARCH EXPRESSION";
		}

		internal void DumpTo(StringBuilder sb) {
			sb.Append(search_expression.Text.ToString());
		}
	}
}