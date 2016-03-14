using System;

using Deveel.Data.Sql.Schemas;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class DropSchemaStatementTests : ContextBasedTest {
		private const string SchemaName = "test";

		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			query.Session.SystemAccess.CreateSchema("test", SchemaTypes.User);
			return query;
		}

		[Test]
		public void DropEmptySchema() {
			var statement = new DropSchemaStatement(SchemaName);

			Query.ExecuteStatement(statement);

			var exists = Query.Session.SystemAccess.SchemaExists(SchemaName);
			Assert.IsFalse(exists);
		}
	}
}
