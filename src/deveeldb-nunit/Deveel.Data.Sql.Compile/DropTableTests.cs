using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DropTableTests : SqlCompileTestBase {
		[Test]
		public void OneTable() {
			const string sql = "DROP TABLE test1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropTableStatement>(statement);

			var dropTable = (DropTableStatement) statement;

			Assert.IsNotNull(dropTable.TableName);
			Assert.IsFalse(dropTable.IfExists);

			Assert.AreEqual("test1", dropTable.TableName.FullName);
		}

		[Test]
		public void OneTableIfExists() {
			const string sql = "DROP TABLE IF EXISTS test2";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropTableStatement>(statement);

			var dropTable = (DropTableStatement)statement;

			Assert.IsNotNull(dropTable.TableName);
			Assert.IsTrue(dropTable.IfExists);

			Assert.AreEqual("test2", dropTable.TableName.FullName);
		}

		[Test]
		public void MultipleTables() {
			const string sql = "DROP TABLE test1, APP.test2";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(2, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropTableStatement>(statement);

			var dropTable = (DropTableStatement)statement;

			Assert.IsNotNull(dropTable.TableName);
			Assert.IsFalse(dropTable.IfExists);

			Assert.AreEqual("test1", dropTable.TableName.FullName);
		}

		[Test]
		public void MultipleTablesIfExist() {
			const string sql = "DROP TABLE IF EXISTS test1, test2";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(2, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropTableStatement>(statement);

			var dropTable = (DropTableStatement)statement;

			Assert.IsNotNull(dropTable.TableName);
			Assert.IsTrue(dropTable.IfExists);

			Assert.AreEqual("test1", dropTable.TableName.FullName);

		}
	}
}
