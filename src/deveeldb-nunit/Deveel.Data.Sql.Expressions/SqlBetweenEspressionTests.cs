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

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public class SqlBetweenEspressionTests : SqlExpressionTestBase {
		[Test]
		public void BetweenNumerics() {
			const string sql = "22 BETWEEN 10 AND 54";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(sql));
			Assert.IsNotNull(expression);

			Assert.IsInstanceOf<SqlBinaryExpression>(expression);
			Assert.AreEqual(SqlExpressionType.And, expression.ExpressionType);

			SqlExpression resultExpression = null;
			Assert.DoesNotThrow(() => resultExpression = expression.Evaluate());
			Assert.IsNotNull(resultExpression);
			Assert.AreEqual(SqlExpressionType.Constant, resultExpression.ExpressionType);

			var value = ((SqlConstantExpression) resultExpression).Value;

			Assert.IsInstanceOf<BooleanType>(value.Type);
			Assert.AreEqual(SqlBoolean.True, (SqlBoolean)value.Value);
		}

		[Test]
		public void BetweenDates() {
			const string sql = "TODATE('2001-02-12') BETWEEN TODATE('2000-01-20') AND TODATE('2003-01-01')";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(sql));
			Assert.IsNotNull(expression);

			Assert.IsInstanceOf<SqlBinaryExpression>(expression);
			Assert.AreEqual(SqlExpressionType.And, expression.ExpressionType);

			SqlExpression resultExpression = null;
			Assert.DoesNotThrow(() => resultExpression = expression.Evaluate());
			Assert.IsNotNull(resultExpression);
			Assert.AreEqual(SqlExpressionType.Constant, resultExpression.ExpressionType);

			var value = ((SqlConstantExpression)resultExpression).Value;

			Assert.IsInstanceOf<BooleanType>(value.Type);
			Assert.AreEqual(SqlBoolean.True, (SqlBoolean)value.Value);
		}
	}
}
