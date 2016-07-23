using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CaseTests : SqlCompileTestBase {
		[Test]
		public void SimpleCaseExpression() {
			const string sql = "SELECT CASE a WHEN 1 THEN 'one' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement) statement;

			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlConditionalExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var conditional = (SqlConditionalExpression) select.QueryExpression.SelectColumns.ElementAt(0).Expression;

			Assert.IsNotNull(conditional);
		}

		[Test]
		public void SimpleCaseExpressionWithTwoOptions() {
			const string sql = "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlConditionalExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var conditional = (SqlConditionalExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;

			Assert.IsNotNull(conditional);
		}

		[Test]
		public void SimpleCaseExpressionWithTwoOptionsAndElse() {
			const string sql = "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'invalid' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlConditionalExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var conditional = (SqlConditionalExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;

			Assert.IsNotNull(conditional);
		}


		[Test]
		public void SearchedCaseExpression() {
			const string sql = "SELECT CASE WHEN a = 1 THEN 'one' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlConditionalExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var conditional = (SqlConditionalExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;

			Assert.IsNotNull(conditional);
		}

		[Test]
		public void SearchedCaseExpressionWithTwoOptions() {
			const string sql = "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlConditionalExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var conditional = (SqlConditionalExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;

			Assert.IsNotNull(conditional);
		}

		[Test]
		public void SearchedCaseExpressionWithTwoOptionsAndElse() {
			const string sql = "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'invalid' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;

			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
			Assert.IsInstanceOf<SqlConditionalExpression>(select.QueryExpression.SelectColumns.ElementAt(0).Expression);

			var conditional = (SqlConditionalExpression)select.QueryExpression.SelectColumns.ElementAt(0).Expression;

			Assert.IsNotNull(conditional);
		}

		[Test]
		public void SimpleCaseStatement() {
			const string sql = "CASE a WHEN 1 THEN RETURN 'one' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<ConditionStatement>(statement);
		}

		[Test]
		public void SimpleCaseStatementWithTwoOptions() {
			const string sql = "CASE a WHEN 1 THEN RETURN 'one' WHEN 2 THEN RETURN 'two' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<ConditionStatement>(statement);
		}

		[Test]
		public void SimpleCaseStatementWithTwoOptionsAndElse() {
			const string sql = "CASE a WHEN 1 THEN RETURN 'one' WHEN 2 THEN RETURN 'two' ELSE RETURN 'invalid' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<ConditionStatement>(statement);
		}

		[Test]
		public void SearchedCaseStatement() {
			const string sql = "CASE WHEN a = 1 THEN RETURN 'one' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<ConditionStatement>(statement);
		}

		[Test]
		public void SearchedCaseStatementWithTwoOptions() {
			const string sql = "CASE WHEN a = 1 THEN RETURN 'one' WHEN a = 2 THEN RETURN 'two' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<ConditionStatement>(statement);
		}

		[Test]
		public void SearchedCaseStatementWithTwoOptionsAndElse() {
			const string sql = "CASE WHEN a = 1 THEN RETURN 'one' WHEN a = 2 THEN RETURN 'two' ELSE RETURN 'invalid' END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<ConditionStatement>(statement);
		}
	}
}
