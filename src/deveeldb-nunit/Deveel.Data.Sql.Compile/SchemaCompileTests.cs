using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class SchemaCompileTests : SqlCompileTestBase {
		[Test]
		public void CreateSchema() {
			const string sql = "CREATE SCHEMA test_schema";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.CodeObjects);
			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateSchemaStatement>(statement);

			var schemaStatement = (CreateSchemaStatement) statement;
			Assert.AreEqual("test_schema", schemaStatement.SchemaName);
		}

		[Test]
		public void DropSchema() {
			const string sql = "DROP SCHEMA test_schema";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.CodeObjects);
			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.FirstOrDefault();

			Assert.IsNotNull(statement);

			Assert.IsInstanceOf<DropSchemaStatement>(statement);

			var schemaStatement = (DropSchemaStatement) statement;
			Assert.AreEqual("test_schema", schemaStatement.SchemaName);
		}
	}
}
