using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CreateUserTests : SqlCompileTestBase {
		[Test]
		public void IdentifiedByPassword() {
			const string sql = "CREATE USER test IDENTIFIED BY PASSWORD '123456789';";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateUserStatement>(statement);

			var createUser = (CreateUserStatement) statement;

			Assert.AreEqual("test", createUser.UserName);
			Assert.IsInstanceOf<SqlConstantExpression>(createUser.Password);
		}
	}
}
