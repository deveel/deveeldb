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

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// All the possile type of <see cref="SqlExpression"/> supported
	/// </summary>
	public enum SqlExpressionType {
		/// <summary>
		/// Represents an expression which has a constant value
		/// </summary>
		/// <seealso cref="SqlConstantExpression"/>
		Constant,

		FunctionCall,
		Cast,
		Conditional,

		// Logical / Conditional Binary
		And,
		Or,
		XOr,

		// Sub-query 

		AnyEqual,
		AnyNotEqual,
		AnyGreaterThan,
		AnyGreaterOrEqualThan,
		AnySmallerThan,
		AnySmallerOrEqualThan,

		AllEqual,
		AllNotEqual,
		AllGreaterThan,
		AllGreaterOrEqualThan,
		AllSmallerThan,
		AllSmallerOrEqualThan,

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
		SmallerThan,
		GreaterThan,
		SmallerOrEqualThan,
		GreaterOrEqualThan,
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
		VariableReference,

		/// <summary>
		/// A variable or reference assignment expression.
		/// </summary>
		Assign,
		Tuple,

		/// <summary>
		/// A query to the database to select data from
		/// a set of tables and columns.
		/// </summary>
		Query,

		/// <summary>
		/// A query that was prepared and ready to be executed
		/// </summary>
		PreparedQuery
	}
}