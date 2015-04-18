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
	/// Handles expressions computed agaist an unary operator.
	/// </summary>
	[Serializable]
	public sealed class SqlUnaryExpression : SqlExpression {
		private readonly SqlExpressionType expressionType;

		internal SqlUnaryExpression(SqlExpressionType expressionType, SqlExpression operand) {
			this.expressionType = expressionType;
			Operand = operand;
		}

		/// <summary>
		/// Gets the operand expression that is computed.
		/// </summary>
		public SqlExpression Operand { get; private set; }

		/// <inheritdoc/>
		public override SqlExpressionType ExpressionType {
			get { return expressionType; }
		}

		/// <inheritdoc/>
		public override bool CanEvaluate {
			get { return true; }
		}
	}
}