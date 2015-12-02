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

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateSchemaStatement>(statement);

			var schemaStatement = (CreateSchemaStatement) statement;
			Assert.AreEqual("test_schema", schemaStatement.SchemaName);
		}
	}
}
