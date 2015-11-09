using System;
using System.Linq;

using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class GrantTests : ContextBasedTest {
		protected override IQueryContext CreateQueryContext(IDatabase database) {
			var context = base.CreateQueryContext(database);
			context.CreateUser("test_user", "12345");
			return context;
		}

		[Test]
		public void ParseGrantToOneUserToOneTable() {
			const string sql = "GRANT SELECT, DELETE, UPDATE PRIVILEGE ON test_table TO test_user";

			var statements = SqlStatement.Parse(sql);
			Assert.IsNotNull(statements);
			Assert.IsNotEmpty(statements);
			Assert.AreEqual(3, statements.Count());
			Assert.IsInstanceOf<GrantPrivilegesStatement>(statements.ElementAt(0));
			Assert.IsInstanceOf<GrantPrivilegesStatement>(statements.ElementAt(1));
			Assert.IsInstanceOf<GrantPrivilegesStatement>(statements.ElementAt(2));
		}
	}
}
