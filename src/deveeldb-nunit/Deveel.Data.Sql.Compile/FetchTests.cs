using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class FetchTests : SqlCompileTestBase {
		[Test]
		public void FetchNextFromCursorInto() {
			const string sql = "FETCH NEXT FROM test_cursor INTO test_table";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<FetchIntoStatement>(statement);

			var cursorStatement = (FetchIntoStatement) statement;
			Assert.AreEqual("test_cursor", cursorStatement.CursorName);
		}

		[Test]
		public void FetchNextImplicitCursor() {
			const string sql = "FETCH NEXT";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<FetchStatement>(statement);

			var cursorStatement = (FetchStatement) statement;
			Assert.IsNullOrEmpty(cursorStatement.CursorName);
		}
	}
}