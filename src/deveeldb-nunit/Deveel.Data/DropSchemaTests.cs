using System;

using Deveel.Data.Sql;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropSchemaTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateSchema("test", SchemaTypes.User);
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			return true;
		}

		[Test]
		public void DropEmptySchema() {
			Query.DropSchema("test");

			var exists = Query.Session.Access().SchemaExists("test");
			Assert.IsFalse(exists);
		}
	}
}
