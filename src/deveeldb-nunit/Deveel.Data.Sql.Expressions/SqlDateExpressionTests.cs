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

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public class SqlDateExpressionTests {
		[Test]
		public void DateSmallerThanOtherDate() {
			const string text = "TODATE('2013-03-01') < TODATE('2013-05-02')";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(text));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlBinaryExpression>(expression);

			SqlExpression evaluated = null;
			Assert.DoesNotThrow(() => evaluated = expression.Evaluate());
			Assert.IsNotNull(evaluated);

			Assert.IsInstanceOf<SqlConstantExpression>(evaluated);

			var value = ((SqlConstantExpression) evaluated).Value;

			Assert.IsInstanceOf<SqlBoolean>(value.Value);

			Assert.AreEqual(SqlBoolean.True, value.Value);
		}
	}
}
