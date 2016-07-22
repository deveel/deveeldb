using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DropRoleTests : SqlCompileTestBase {
		[Test]
		public void DropOneRole() {
			const string sql = "DROP ROLE dbadmin";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropRoleStatement>(statement);

			var create = (DropRoleStatement)statement;

			Assert.AreEqual("dbadmin", create.RoleName);

		}
	}
}
