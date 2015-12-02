using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class CursorCompileTests : SqlCompileTestBase {
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

		[Test]
		public void OpenCursorWithArguments() {
			const string sql = "OPEN test_cursor (22, b)";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<OpenStatement>(statement);

			var cursorStatement = (OpenStatement) statement;
			Assert.AreEqual("test_cursor", cursorStatement.CursorName);
			Assert.IsNotEmpty(cursorStatement.Arguments);
		}

		[Test]
		public void OpenCursorSimple() {
			const string sql = "OPEN test_cursor";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<OpenStatement>(statement);

			var cursorStatement = (OpenStatement) statement;
			Assert.AreEqual("test_cursor", cursorStatement.CursorName);
			Assert.IsEmpty(cursorStatement.Arguments);
		}

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
			Assert.IsInstanceOf<FetchStatement>(statement);

			var cursorStatement = (FetchStatement) statement;
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
