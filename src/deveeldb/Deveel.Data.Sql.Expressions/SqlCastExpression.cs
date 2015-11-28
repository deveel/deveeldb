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

using Deveel.Data.Serialization;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// An <see cref="SqlExpression"/> that will cast a value retrieved by
	/// the evaluation of another expression into a given SQL data type.
	/// </summary>
	[Serializable]
	public sealed class SqlCastExpression : SqlExpression {
		/// <summary>
		/// Constructs the expression with the a given value to be converted
		/// and the conversion type.
		/// </summary>
		/// <param name="value">The expression whose value resulted from its
		/// evaluation will be casted to the given type.</param>
		/// <param name="sqlType">The destination type this expression
		/// will convert to.</param>
		public SqlCastExpression(SqlExpression value, SqlType sqlType) {
			SqlType = sqlType;
			Value = value;
		}

		private SqlCastExpression(ObjectData data) {
			Value = data.GetValue<SqlExpression>("Value");
			SqlType = data.GetValue<SqlType>("Type");
		}

		/// <summary>
		/// Gets the expression whose evaluated value will be converted.
		/// </summary>
		public SqlExpression Value { get; private set; }

		/// <summary>
		/// Gets the destination type of the conversion
		/// </summary>
		/// <seealso cref="SqlType.CastTo"/>
		/// <seealso cref="SqlType.CanCastTo"/>
		public SqlType SqlType { get; private set; }

		/// <inheritdoc/>
		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Cast; }
		}

		/// <inheritdoc/>
		public override bool CanEvaluate {
			get { return true; }
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("Value", Value);
			data.SetValue("Type", SqlType);
		}
	}
}