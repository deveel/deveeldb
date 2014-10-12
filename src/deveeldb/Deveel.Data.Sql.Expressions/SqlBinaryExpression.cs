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
	[Serializable]
	public abstract class SqlBinaryExpression : SqlExpression {
		protected SqlBinaryExpression(SqlExpression left, SqlExpression right) {
			Left = left;
			Right = right;
		}

		public SqlExpression Left { get; private set; }

		public SqlExpression Right { get; private set; }

		protected abstract DataObject EvaluateBinary(DataObject left, DataObject right);

		public override bool CanEvaluate {
			get { return true; }
		}

		public override SqlExpression Evaluate(EvaluateContext context) {
			var left = (SqlConstantExpression) Left.Evaluate(context);
			var right = (SqlConstantExpression) Right.Evaluate(context);
			var computedValue = EvaluateBinary(left.Value, right.Value);
			return new SqlConstantExpression(computedValue);
		}
	}
}