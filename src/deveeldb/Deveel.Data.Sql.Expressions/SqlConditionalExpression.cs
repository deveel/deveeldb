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
using System.Runtime.CompilerServices;

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlConditionalExpression : SqlExpression {
		public SqlConditionalExpression(SqlExpression testExpression, SqlExpression trueExpression) 
			: this(testExpression, trueExpression, null) {
		}

		public SqlConditionalExpression(SqlExpression testExpression, SqlExpression trueExpression, SqlExpression falsExpression) {
			if (testExpression == null) 
				throw new ArgumentNullException("testExpression");
			if (trueExpression == null) 
				throw new ArgumentNullException("trueExpression");

			TrueExpression = trueExpression;
			TestExpression = testExpression;
			FalseExpression = falsExpression;
		}

		public SqlExpression TestExpression { get; private set; }

		public SqlExpression TrueExpression { get; private set; }

		public SqlExpression FalseExpression { get; set; }

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Conditional; }
		}

		public override bool CanEvaluate {
			get { return true; }
		}

		public override SqlExpression Evaluate(EvaluateContext context) {
			if (IsTrue(context))
				return TrueExpression.Evaluate(context);

			if (FalseExpression != null)
				return FalseExpression.Evaluate(context);

			return Constant(DataObject.Null());
		}

		private bool IsTrue(EvaluateContext context) {
			var test = TestExpression.Evaluate(context);
			if (!(test is SqlConstantExpression))
				throw new ExpressionEvaluateException("The returned type of the test expression is not constant.");

			var constant = (SqlConstantExpression) test;
			var testResult = constant.Value.AsBoolean();
			if (testResult.IsNull)
				throw new ExpressionEvaluateException("The test expression evaluated to NULL.");

			return testResult;
		}
	}
}