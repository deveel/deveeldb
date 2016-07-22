using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CreateRoleTests : SqlCompileTestBase {
		[Test]
		public void CreateOneRole() {
			const string sql = "CREATE ROLE dbadmin";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateRoleStatement>(statement);

			var create = (CreateRoleStatement) statement;

			Assert.AreEqual("dbadmin", create.RoleName);
		}
	}
}
