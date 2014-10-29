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
	/// This is the base class for expressions computed agaist an
	/// unary operator.
	/// </summary>
	[Serializable]
	public abstract class SqlUnaryExpression : SqlExpression {
		/// <summary>
		/// Constructs the expression with a given operand.
		/// </summary>
		/// <param name="operand">The expression as operand of the unary operator.</param>
		protected SqlUnaryExpression(SqlExpression operand) {
			Operand = operand;
		}

		/// <summary>
		/// Gets the operand expression that is computed.
		/// </summary>
		public SqlExpression Operand { get; private set; }

		/// <summary>
		/// When overridden by a derived class, this applies the unary operator
		/// to the constant value, obtained by the reduction to constant in a
		/// previous moment of the expression lifecycle.
		/// </summary>
		/// <param name="value">The constant value to evaluate.</param>
		/// <returns>
		/// Returns the constant result of the application of the unary operator 
		/// to the input value given.
		/// </returns>
		protected abstract DataObject EvaluateUnary(DataObject value);

		/// <inheritdoc/>
		public override bool CanEvaluate {
			get { return true; }
		}

		/// <inheritdoc/>
		public override SqlExpression Evaluate(EvaluateContext context) {
			var exp = (SqlConstantExpression) Operand.Evaluate(context);
			var computedValue = EvaluateUnary(exp.Value);
			return new SqlConstantExpression(computedValue);
		}
	}
}