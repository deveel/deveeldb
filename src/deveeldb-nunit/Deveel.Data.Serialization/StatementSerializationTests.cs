using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Serialization {
	[TestFixture]
	public class StatementSerializationTests : SerializationTestBase {
		[Test]
		public void SerializeSelect() {
			var expression = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM table WHERE a = 1");
			var statement = new SelectStatement(expression);

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsInstanceOf<SelectStatement>(deserialized);

				Assert.IsNotNull(deserialized.QueryExpression);
			});
		}
	}
}
