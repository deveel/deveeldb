//  
//  InsertStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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

namespace Deveel.Data {
	/// <summary>
	/// The class representing a variable within a transaction.
	/// </summary>
	public sealed class Variable {
		/// <summary>
		/// Constructs the variable with a given name and 
		/// </summary>
		/// <param name="name">The simple name of the variable.</param>
		/// <param name="type">The <see cref="TType"/> of the value
		/// handled by the variable.</param>
		/// <param name="constant">The flag indicating if the value handled
		/// by this variable must be constant.</param>
		/// <param name="notNull">The flag indicating if the value handled
		/// by the variable must not be set to <c>NULL</c>.</param>
		internal Variable(string name, TType type, bool constant, bool notNull) {
			if (name == null || name.Length == 0)
				throw new ArgumentNullException("name");
			if (type == null)
				throw new ArgumentNullException("type");

			this.name = name;
			this.type = type;
			this.constant = constant;
			this.notNull = notNull;
		}

		/// <summary>
		/// The simple name of the variable.
		/// </summary>
		private readonly string name;

		/// <summary>
		/// The <see cref="TObject"/> instance that represents
		/// the value of the variable.
		/// </summary>
		private readonly TType type;

		/// <summary>
		/// A flag indicating if the value of the variable must
		/// be constant.
		/// </summary>
		private readonly bool constant;

		/// <summary>
		/// A flag indicating if the value of the variable must
		/// not be evaluated to <c>NULL</c>.
		/// </summary>
		private readonly bool notNull;

		/// <summary>
		/// The value of the variable.
		/// </summary>
		private TObject value = TObject.Null;

		/// <summary>
		/// The flag indicating if the variable was set.
		/// </summary>
		private bool is_set;

		/// <summary>
		/// The original expression that represents the value set to 
		/// the variable.
		/// </summary>
		internal Expression original_expression;

		/// <summary>
		/// Gets a boolean value indicating if the variable value was set.
		/// </summary>
		public bool IsSet {
			get { return is_set; }
		}

		/// <summary>
		/// Gets the <see cref="TType"/> of the value handled
		/// by the variable.
		/// </summary>
		public TType Type {
			get { return type; }
		}

		/// <summary>
		/// Gets the name of the variable.
		/// </summary>
		/// <remarks>
		/// This is the identifier of the variable within the
		/// transaction.
		/// </remarks>
		public string Name {
			get { return name; }
		}

		/// <summary>
		/// Gets or sets the value of the variable.
		/// </summary>
		public TObject Value {
			get { return value; }
		}

		/// <summary>
		/// Gets a boolean value indicating if the variable has a 
		/// constant value.
		/// </summary>
		public bool Constant {
			get { return constant; }
		}

		/// <summary>
		/// Gets a boolean value indicating if the value to set must
		/// not be evaluated to <c>NULL</c>.
		/// </summary>
		public bool NotNull {
			get { return notNull; }
		}

		/// <summary>
		/// Sets the value of the variable from the given expression.
		/// </summary>
		/// <param name="expression">The expression used to set the final
		/// value of the variable.</param>
		/// <param name="context">The context used to resolve the expression
		/// into the final value of the variable.</param>
		public void SetValue(Expression expression, IQueryContext context) {
			if (constant && is_set)
				throw new InvalidOperationException("The variable '" + name + "' is constant and the value was already set.");

			if (expression == null)
				throw new ArgumentNullException("expression");

			original_expression = (Expression) expression.Clone();
			value = expression.Evaluate(null, null, context);

			try {
				if (!type.Equals(value.TType))
					value = value.CastTo(type);
			} catch(Exception) {
				throw new InvalidOperationException("It was not possible to convert the value type " + value.TType +
				                                    " to the variable type " + type + ".");
			}

			if (value.IsNull && notNull)
				throw new InvalidOperationException("The constant is marked as NOT NULL and a NULL value was assigned.");

			is_set = true;
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder sb = new StringBuilder(name);
			if (constant)
				sb.Append(" CONSTANT");
			sb.Append(type.ToSQLString());
			if (notNull)
				sb.Append(" NOT NULL");
			return sb.ToString();
		}
	}
}