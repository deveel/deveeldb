using System;

using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class DropUserStatementTests : ContextBasedTest {
		private const string UserName = "tester";

		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			CreateUser(query);
			return query;
		}

		private void CreateUser(IQuery query) {
			query.CreateUser(UserName, "12345");
		}

		[Test]
		public void DropExisting() {
			var statement = new DropUserStatement(UserName);

			Query.ExecuteStatement(statement);

			var exists = Query.UserExists(UserName);
			Assert.IsFalse(exists);
		}
	}
}
