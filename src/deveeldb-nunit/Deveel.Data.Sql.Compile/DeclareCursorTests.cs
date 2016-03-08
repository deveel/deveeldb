using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DeclareCursorTests : SqlCompileTestBase {
		[Test]
		public void DeclareCursorWithArguments() {
			const string sql = "DECLARE CURSOR test_cursor (a INT, b VARCHAR) IS SELECT * FROM test_table WHERE test_table.a = a";
			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DeclareCursorStatement>(statement);

			var cursorStatement = (DeclareCursorStatement) statement;

			Assert.AreEqual("test_cursor", cursorStatement.CursorName);
			Assert.IsNotEmpty(cursorStatement.Parameters);
			Assert.IsNotNull(cursorStatement.QueryExpression);
		}

		[Test]
		public void ImplicitDeclareCursor() {
			const string sql = "CURSOR test_cursor (a INT, b VARCHAR) IS SELECT * FROM test_table WHERE test_table.a = a";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DeclareCursorStatement>(statement);

			var cursorStatement = (DeclareCursorStatement) statement;

			Assert.AreEqual("test_cursor", cursorStatement.CursorName);
			Assert.IsNotEmpty(cursorStatement.Parameters);
			Assert.IsNotNull(cursorStatement.QueryExpression);
		}

		[Test]
		public void DeclareCursorWithoutArguments() {
			const string sql = "DECLARE CURSOR test_cursor IS SELECT * FROM test_table WHERE test_table.a > 0";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DeclareCursorStatement>(statement);

			var cursorStatement = (DeclareCursorStatement) statement;

			Assert.AreEqual("test_cursor", cursorStatement.CursorName);
			Assert.IsEmpty(cursorStatement.Parameters);
			Assert.IsNotNull(cursorStatement.QueryExpression);
		}

	}
}