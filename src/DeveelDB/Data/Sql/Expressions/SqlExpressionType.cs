// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// All the possible type of <see cref="SqlExpression"/> supported
	/// </summary>
	public enum SqlExpressionType {
		/// <summary>
		/// Represents an expression which has a constant value
		/// </summary>
		/// <seealso cref="SqlConstantExpression"/>
		Constant,

		/// <summary>
		/// A reference to a function defined by the system
		/// </summary>
		Function,

		/// <summary>
		/// The special function for casting values to given types
		/// </summary>
		Cast,

		/// <summary>
		/// A condition that returns as result a value if <c>TRUE</c> or
		/// another value if <c>FALSE</c>.
		/// </summary>
		Condition,

		// Logical / Conditional Binary
		And,
		Or,
		XOr,

		Any,
		All,

		// Multiplicative Binary
		Add,
		Subtract,
		Multiply,
		Divide,
		Modulo,

		// Comparison Binary
		Equal,
		NotEqual,
		Is,
		IsNot,
		LessThan,
		GreaterThan,
		LessThanOrEqual,
		GreaterThanOrEqual,
		Like,
		NotLike,

		// Unary
		Not,
		UnaryPlus,
		Negate,

		/// <summary>
		/// An expression that references an object on the database
		/// (that is a Column, Table, Sequence, another database object).
		/// </summary>
		Reference,

		/// <summary>
		/// References a variable in a context.
		/// </summary>
		Variable,

		/// <summary>
		/// References a parameter in an expression
		/// </summary>
		Parameter,

		/// <summary>
		/// A variable assignment expression.
		/// </summary>
		VariableAssign,

		ReferenceAssign,

		/// <summary>
		/// A group of expressions that is evaluated with a higher
		/// precedence than other expressions in the same context
		/// </summary>
		Group,

		/// <summary>
		/// A query to the database to select data from
		/// a set of tables and columns.
		/// </summary>
		Query,
	}
}