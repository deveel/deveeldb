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
using System.Text;

using Deveel.Data.Sql;

namespace Deveel.Data {
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