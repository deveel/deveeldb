using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectIntoTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			Query.Access().CreateTable(tableInfo);

			var table = Query.Access().GetMutableTable(tableName);

			var row = table.NewRow();
			row.SetValue(0, 13);
			row.SetValue(1, "test1");
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue(0, 38);
			row.SetValue(1, "greetings");
			table.AddRow(row);

			Query.Context.DeclareVariable("a", PrimitiveTypes.String());
			Query.Context.DeclareVariable("b", PrimitiveTypes.Integer());
		}

		protected override void OnTearDown() {
			base.OnTearDown();
		}

		[Test]
		public void OneColumnIntoOneVariable() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT a FROM test_table");
			Query.SelectInto(query, "b");

			var variable = Query.Context.FindVariable("b");

			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<NumericType>(variable.Type);
			Assert.IsFalse(variable.Value.IsNull);
			Assert.IsInstanceOf<SqlNumber>(variable.Value.Value);

			var number = (SqlNumber) variable.Value.Value;
			Assert.AreEqual(new SqlNumber(13), number);
		}

		[Test]
		public void TwoColumnsIntoTwoVariables() {
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT a, b FROM test_table");
			Query.SelectInto(query, "b", "a");

			var variable = Query.Context.FindVariable("b");

			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<NumericType>(variable.Type);
			Assert.IsFalse(variable.Value.IsNull);
			Assert.IsInstanceOf<SqlNumber>(variable.Value.Value);

			var number = (SqlNumber)variable.Value.Value;
			Assert.AreEqual(new SqlNumber(13), number);
		}
	}
}
