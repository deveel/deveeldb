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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Search expression is a form of an <see cref="Expression"/> that is 
	/// split up into component parts that can be easily formed into a 
	/// search command.
	/// </summary>
	[Serializable]
	public sealed class SearchExpression : IStatementTreeObject {
		/// <summary>
		/// Constructs a new <see cref="SearchExpression"/> that encapsulates
		/// the given <see cref="Expression"/>.
		/// </summary>
		/// <param name="expression">The <see cref="Expression"/> used to search
		/// in a <c>SELECT</c> statement.</param>
		public SearchExpression(Expression expression) {
			this.expression = expression;
		}

		/// <summary>
		/// The originating expression.
		/// </summary>
		private Expression expression;

		/// <summary>
		/// Gets this search expression from the given expression.
		/// </summary>
		public Expression FromExpression {
			get { return expression; }
			internal set { expression = value; }
		}

		/// <summary>
		/// Concatenates a new expression to the end of this expression 
		/// and uses the <c>AND</c> operator to seperate the expressions.
		/// </summary>
		/// <param name="exp"></param>
		/// <remarks>
		/// This is very useful for adding new logical conditions to the 
		/// expression at runtime.
		/// </remarks>
		internal void AppendExpression(Expression exp) {
			expression = expression == null ? exp : new Expression(expression, Operator.And, exp);
		}


		///<summary>
		/// Prepares the expression.
		///</summary>
		///<param name="preparer"></param>
		internal void Prepare(IExpressionPreparer preparer) {
			if (expression != null) {
				expression.Prepare(preparer);
			}
		}

		/// <inheritdoc/>
		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			Prepare(preparer);
		}

		/// <inheritdoc/>
		public Object Clone() {
			SearchExpression v = (SearchExpression)MemberwiseClone();
			if (expression != null) {
				v.expression = (Expression)expression.Clone();
			}
			return v;
		}


		internal void DumpTo(StringBuilder sb) {
			sb.Append(expression.Text.ToString());
		}
	}
}