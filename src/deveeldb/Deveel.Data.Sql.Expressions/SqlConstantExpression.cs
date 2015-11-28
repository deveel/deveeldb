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
	/// An expression that holds a constant value.
	/// </summary>
	/// <remarks>
	/// As constant, this expression cannot be reduced, so
	/// that <see cref="SqlExpression.CanEvaluate"/> will always
	/// return <c>false</c> and the value of <see cref="SqlExpression.Evaluate(EvaluateContext)"/>
	/// will return the expression itself.
	/// </remarks>
	[Serializable]
	public sealed class SqlConstantExpression : SqlExpression {
		internal SqlConstantExpression(DataObject value) {
			Value = value;
		}

		private SqlConstantExpression(ObjectData data)
			: base(data) {
			Value = data.GetValue<DataObject>("Value");
		}

		/// <summary>
		/// Gets the SQL expression type of <see cref="SqlExpressionType.Constant"/>
		/// </summary>
		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Constant; }
		}

		public override bool CanEvaluate {
			get {
				if (Value.Type is ArrayType ||
				    Value.Type is QueryType)
					return true;

				return false;
			}
		}

		/// <summary>
		/// Gets the constant value of the expression.
		/// </summary>
		public DataObject Value { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("Value", typeof(DataObject), Value);
		}
	}
}