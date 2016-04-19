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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Serialization {
	[TestFixture]
	public class StatementSerializationTests : SerializationTestBase {
		[Test]
		public void SerializeSelect() {
			var expression = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM table1 WHERE a = 1");
			var statement = new SelectStatement(expression);

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsInstanceOf<SelectStatement>(deserialized);

				Assert.IsNotNull(deserialized.QueryExpression);
			});
		}

		[Test]
		public void Goto() {
			var statement = new GoToStatement("test");

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.AreEqual(serialized.Label, deserialized.Label);
			});
		}

		[Test]
		public void Exit() {
			var statement = new ExitStatement();
			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNull(deserialized.Label);
				Assert.IsNull(serialized.WhenExpression);
			});
		}

		[Test]
		public void ExitLabel() {
			var statement = new ExitStatement("test");
			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.Label);
				Assert.AreEqual(serialized.Label, deserialized.Label);
				Assert.IsNull(deserialized.WhenExpression);
			});
		}

		[Test]
		public void ExitWhen() {
			var statement = new ExitStatement(SqlExpression.Constant(true));
			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNull(deserialized.Label);
				Assert.IsNotNull(deserialized.WhenExpression);
				Assert.IsInstanceOf<SqlConstantExpression>(deserialized.WhenExpression);
			});
		}
	}
}
