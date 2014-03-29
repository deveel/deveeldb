// 
//  Copyright 2010-2013 Deveel
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
	/// An assignment from a variable to an expression.
	/// </summary>
	/// <remarks>
	/// 
	/// </remarks>
	/// <example>
	/// For example:
	/// <code>
	/// value_of = value_of * 1.10
	/// name = concat("CS-", name)
	/// description = concat("LEGACY: ", upper(number));
	/// </code>
	/// </example>
	[Serializable]
	public sealed class Assignment : IStatementTreeObject {
		/// <summary>
		/// The <see cref="VariableName"/> that is the lhs of the assignment.
		/// </summary>
		private VariableName variable;

		/// <summary>
		/// Set expression that is the rhs of the assignment.
		/// </summary>
		private Expression expression;

		/// <summary>
		/// Constructs the assignment given a variable name and
		/// an expression.
		/// </summary>
		/// <param name="variable"></param>
		/// <param name="expression"></param>
		public Assignment(VariableName variable, Expression expression) {
			this.variable = variable;
			this.expression = expression;
		}

		/// <summary>
		/// Returns the variable for this assignment.
		/// </summary>
		public VariableName VariableName {
			get { return variable; }
		}

		/// <summary>
		/// Returns the Expression for this assignment.
		/// </summary>
		public Expression Expression {
			get { return expression; }
		}

		/// <inheritdoc/>
		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			if (expression != null)
				expression.Prepare(preparer);
		}

		/// <inheritdoc/>
		public object Clone() {
			Assignment v = (Assignment)MemberwiseClone();
			v.variable = (VariableName)variable.Clone();
			v.expression = (Expression)expression.Clone();
			return v;
		}
	}
}