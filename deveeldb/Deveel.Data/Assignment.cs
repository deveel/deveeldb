//  
//  Assignment.cs
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

using Deveel.Data.Sql;

namespace Deveel.Data {
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
		/// The <see cref="Data.Variable"/> that is the lhs of the assignment.
		/// </summary>
		private Variable variable;

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
		public Assignment(Variable variable, Expression expression) {
			this.variable = variable;
			this.expression = expression;
		}

		/// <summary>
		/// Returns the variable for this assignment.
		/// </summary>
		public Variable Variable {
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
			v.variable = (Variable)variable.Clone();
			v.expression = (Expression)expression.Clone();
			return v;
		}
	}
}