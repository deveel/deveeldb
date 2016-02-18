using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class CreateViewTests : SqlCompileTestBase {
		[Test]
		public void WithColumns() {
			const string sql = "CREATE VIEW test_view (a, b, c) AS SELECT * FROM test";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.First();

			Assert.IsInstanceOf<CreateViewStatement>(statement);

			var createView = (CreateViewStatement) statement;
			Assert.AreEqual("test_view", createView.ViewName);
			Assert.IsNotEmpty(createView.ColumnNames);
			Assert.IsNotNull(createView.QueryExpression);
			Assert.IsFalse(createView.ReplaceIfExists);
		}

		[Test]
		public void WithoutColumns() {
			const string sql = "CREATE VIEW test_view AS SELECT a, c FROM test";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.First();

			Assert.IsInstanceOf<CreateViewStatement>(statement);

			var createView = (CreateViewStatement)statement;
			Assert.AreEqual("test_view", createView.ViewName);
			Assert.IsNull(createView.ColumnNames);
			Assert.IsNotNull(createView.QueryExpression);
			Assert.IsFalse(createView.ReplaceIfExists);
		}

		[Test]
		public void OrReplace() {
			const string sql = "CREATE OR REPLACE VIEW text_view1 AS SELECT * FROM test_table WHERE a = 1";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.First();

			Assert.IsInstanceOf<CreateViewStatement>(statement);

			var createView = (CreateViewStatement)statement;

			Assert.IsTrue(createView.ReplaceIfExists);
		}
	}
}
