using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class SelectIntoTests : SqlCompileTestBase {
		[Test]
		public void SelectIntoTable() {
			const string sql = "SELECT a INTO table2 FROM test_table";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectIntoStatement>(statement);

			var selectInto = (SelectIntoStatement) statement;
			Assert.IsNotNull(selectInto);
			Assert.IsNotNull(selectInto.QueryExpression);
			Assert.IsNotNull(selectInto.Reference);
			Assert.IsInstanceOf<SqlReferenceExpression>(selectInto.Reference);
		}

		[Test]
		public void SelectIntoOneVariable() {
			const string sql = "SELECT a INTO :a FROM test_table";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectIntoStatement>(statement);

			var selectInto = (SelectIntoStatement)statement;
			Assert.IsNotNull(selectInto);
			Assert.IsNotNull(selectInto.QueryExpression);
			Assert.IsNotNull(selectInto.Reference);
			Assert.IsInstanceOf<SqlTupleExpression>(selectInto.Reference);
		}
	}
}
