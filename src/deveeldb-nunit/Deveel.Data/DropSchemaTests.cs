using System;

using Deveel.Data.Sql;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropSchemaTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			Query.Session.Access().CreateSchema("test", SchemaTypes.User);
		}

		[Test]
		public void DropEmptySchema() {
			Query.DropSchema("test");

			var exists = Query.Session.Access().SchemaExists("test");
			Assert.IsFalse(exists);
		}
	}
}
