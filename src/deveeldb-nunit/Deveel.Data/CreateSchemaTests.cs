using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateSchemaTests : ContextBasedTest {
		[Test]
		public void CreateNewSchema() {
			const string schemaName = "Sch1";

			Query.CreateSchema(schemaName);

			var exists = Query.Session.Access.SchemaExists(schemaName);

			Assert.IsTrue(exists);
		}
	}
}
