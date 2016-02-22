using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class ShowTests : SqlCompileTestBase {
		[Test]
		public void ShowTables() {
			const string sql = "SHOW TABLES";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<ShowStatement>(statement);

			var show = (ShowStatement) statement;

			Assert.AreEqual(ShowTarget.SchemaTables, show.Target);
		}

		[Test]
		public void ShowSchema() {
			const string sql = "SHOW SCHEMA";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<ShowStatement>(statement);

			var show = (ShowStatement)statement;

			Assert.AreEqual(ShowTarget.Schema, show.Target);
		}


		[Test]
		public void ShowTable() {
			const string sql = "SHOW TABLE APP.test";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<ShowStatement>(statement);

			var show = (ShowStatement)statement;

			Assert.AreEqual(ShowTarget.Table, show.Target);
			Assert.IsNotNull(show.TableName);
			Assert.AreEqual("APP", show.TableName.ParentName);
			Assert.AreEqual("test", show.TableName.Name);
		}
	}
}
