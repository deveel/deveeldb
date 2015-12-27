using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CloseTests : SqlCompileTestBase {
		[Test]
		public void CloseCursor() {
			const string sql = "CLOSE test_cursor";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CloseStatement>(statement);

			var cursorStatement = (CloseStatement) statement;
			Assert.AreEqual("test_cursor", cursorStatement.CursorName);
		}
	}
}