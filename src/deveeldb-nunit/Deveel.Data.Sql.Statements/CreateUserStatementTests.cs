using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class CreateUserStatementTests : ContextBasedTest {
		[Test]
		public void ParseSimpleCreateUser() {
			const string sql = "CREATE USER test IDENTIFIED BY PASSWORD '123456789';";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(QueryContext, sql));

			Assert.IsNotNull(statements);

		}
	}
}
