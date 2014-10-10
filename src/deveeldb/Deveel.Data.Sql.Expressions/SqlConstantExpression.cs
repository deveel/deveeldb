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

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// An expression that holds a constant value.
	/// </summary>
	/// <remarks>
	/// As constant, this expression cannot be reduced, so
	/// that <see cref="SqlExpression.CanReduce"/> will always
	/// return <c>false</c> and the value of <see cref="SqlExpression.Reduce"/>
	/// will return the expression itself.
	/// </remarks>
	[Serializable]
	public sealed class SqlConstantExpression : SqlExpression {
		/// <summary>
		/// Constructs the expression given a constant value
		/// </summary>
		/// <param name="value">The value held by the expression</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="value"/> is <c>null</c>.
		/// </exception>
		public SqlConstantExpression(TObject value) {
			Value = value;
		}

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Constant; }
		}

		/// <summary>
		/// Gets the constant value of the expression.
		/// </summary>
		public TObject Value { get; private set; }
	}
}