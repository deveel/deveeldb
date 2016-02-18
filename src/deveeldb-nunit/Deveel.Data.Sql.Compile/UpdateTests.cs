using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class UpdateTests : SqlCompileTestBase {
		[Test]
		public void SimpleUpdate() {
			const string sql = "UPDATE table SET col1 = 'testUpdate', col2 = 22 WHERE id = 1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<UpdateStatement>(statement);

			var update = (UpdateStatement) statement;
			Assert.AreEqual("table", update.TableName);

			Assert.IsNotEmpty(update.Assignments);
			Assert.IsNotNull(update.WherExpression);
			Assert.AreEqual(-1, update.Limit);
		}

		[Test]
		public void SimpleUpdateWithLimit() {
			const string sql = "UPDATE table SET col1 = 'testUpdate', col2 = 22 WHERE id = 1 LIMIT 20";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<UpdateStatement>(statement);

			var update = (UpdateStatement) statement;
			Assert.AreEqual("table", update.TableName);

			Assert.IsNotEmpty(update.Assignments);
			Assert.IsNotNull(update.WherExpression);
			Assert.AreEqual(20, update.Limit);
		}
	}
}