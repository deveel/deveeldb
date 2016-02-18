using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DropViewTests : SqlCompileTestBase {
		[Test]
		public void OneView() {
			const string sql = "DROP VIEW view1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			
			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropViewStatement>(statement);

			var dropView = (DropViewStatement) statement;

			Assert.IsFalse(dropView.IfExists);
			Assert.AreEqual("view1", dropView.ViewName.FullName);
		}

		[Test]
		public void OneViewIfExists() {
			const string sql = "DROP VIEW IF EXISTS view1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropViewStatement>(statement);

			var dropView = (DropViewStatement)statement;

			Assert.IsTrue(dropView.IfExists);
			Assert.AreEqual("view1", dropView.ViewName.FullName);
		}

		[Test]
		public void MultipleViews() {
			const string sql = "DROP VIEW view1, APP.view2";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(2, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropViewStatement>(statement);

			var dropView = (DropViewStatement)statement;

			Assert.IsFalse(dropView.IfExists);
			Assert.AreEqual("view1", dropView.ViewName.FullName);
		}
	}
}
