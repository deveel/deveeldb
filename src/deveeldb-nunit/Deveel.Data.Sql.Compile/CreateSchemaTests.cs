using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CreateSchemaTests : SqlCompileTestBase {
		[Test]
		public void SimpleSchema() {
			const string sql = "CREATE SCHEMA schema1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateSchemaStatement>(statement);

			var createSchema = (CreateSchemaStatement) statement;

			Assert.AreEqual("schema1", createSchema.SchemaName);
		}
	}
}