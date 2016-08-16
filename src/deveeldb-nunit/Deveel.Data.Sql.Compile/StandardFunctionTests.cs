using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class StandardFunctionTests : SqlCompileTestBase {
		[Test]
		public void CastNumberToVarchar() {
			const string sql = "SELECT CAST(8763 AS VARCHAR)";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());

			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_CAST", function.FunctioName.FullName);
		}

		[Test]
		public void CastStringToNumeric() {
			const string sql = "SELECT CAST('8763' AS NUMERIC(3))";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression) select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_CAST", function.FunctioName.FullName);
		}

		[Test]
		public void CurrentTime() {
			const string sql = "SELECT CURRENT_TIME";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("TIME", function.FunctioName.FullName);
		}

		[Test]
		public void CurrentTimeStamp() {
			const string sql = "SELECT CURRENT_TIMESTAMP";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("TIMESTAMP", function.FunctioName.FullName);
		}

		[Test]
		public void ToTimeStamp_NoTimeZone() {
			const string sql = "SELECT TIMESTAMP :a";
			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("TOTIMESTAMP", function.FunctioName.FullName);
			Assert.IsNotNull(function.Arguments);
			Assert.AreEqual(1, function.Arguments.Length);

			var arg = function.Arguments[0];
			Assert.IsNotNull(arg);
			Assert.IsInstanceOf<SqlVariableReferenceExpression>(arg.Value);
		}

		[Test]
		public void ToTimeStamp_WithTimeZone() {
			const string sql = "SELECT TIMESTAMP '2016-05-20T05:33:22' AT TIME ZONE 'CET'";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.IsNotNull(function.Arguments);
			Assert.AreEqual(2, function.Arguments.Length);

			var arg1 = function.Arguments[0];
			Assert.IsNotNull(arg1);
			Assert.IsInstanceOf<SqlConstantExpression>(arg1.Value);

			var arg2 = function.Arguments[1];
			Assert.IsNotNull(arg2);
			Assert.IsInstanceOf<SqlConstantExpression>(arg2.Value);
		}

		[Test]
		public void NextValueFor() {
			const string sql = "SELECT NEXT VALUE FOR seq1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("NEXTVAL", function.FunctioName.FullName);
		}

		[TestCase("DAY")]
		[TestCase("YEAR")]
		[TestCase("MONTH")]
		public void Extract(string part) {
			var sql = String.Format("SELECT EXTRACT({0} FROM '2016-05-14')", part);

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_EXTRACT", function.FunctioName.FullName);
		}

		[Test]
		public void BasicTrim() {
			const string sql = "SELECT TRIM('input ')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_TRIM", function.FunctioName.FullName);
		}

		[Test]
		public void TrimLeadingSpaces() {
			const string sql = "SELECT TRIM(LEADING FROM 'input ')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_TRIM", function.FunctioName.FullName);
		}

		[Test]
		public void TrimTrailingSpaces() {
			const string sql = "SELECT TRIM(TRAILING FROM 'input ')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_TRIM", function.FunctioName.FullName);
		}

		[Test]
		public void TrimBothSideSpaces() {
			const string sql = "SELECT TRIM(BOTH FROM 'input ')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_TRIM", function.FunctioName.FullName);
		}

		[Test]
		public void TrimLeadingString() {
			const string sql = "SELECT TRIM(LEADING '0' FROM '002-ex')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_TRIM", function.FunctioName.FullName);
		}

		[Test]
		public void TrimTrailingString() {
			const string sql = "SELECT TRIM(TRAILING '0' FROM '002-ex')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_TRIM", function.FunctioName.FullName);
		}

		[Test]
		public void TrimBothSideString() {
			const string sql = "SELECT TRIM(BOTH '0' FROM '002-ex')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("SQL_TRIM", function.FunctioName.FullName);
		}

		[Test]
		public void InvokedFunction() {
			const string sql = "SELECT fun1('arg1', 344.001)";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlFunctionCallExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var function = (SqlFunctionCallExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;
			Assert.AreEqual("fun1", function.FunctioName.FullName);
		}
	}
}
