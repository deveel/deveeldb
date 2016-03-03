using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class DeleteStatementTests : ContextBasedTest {
		private void CreateTestTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.CreateTable(tableInfo);
			query.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		private void InsertTestData(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");

			var table = query.GetMutableTable(tableName);
			var row = table.NewRow();
			row.SetValue("first_name", Field.String("Antonello"));
			row.SetValue("last_name", Field.String("Provenzano"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1980, 06, 04)));
			row.SetValue("active", Field.BooleanTrue);
			row.SetDefault(query);
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue("first_name", Field.String("Sebastiano"));
			row.SetValue("last_name", Field.String("Provenzano"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1981, 08, 26)));
			row.SetValue("active", Field.BooleanFalse);
			row.SetDefault(query);
			table.AddRow(row);
		}

		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			CreateTestTable(query);
			InsertTestData(query);
			return query;
		}

		[Test]
		public void DeleteOnlyOneRow() {
			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("first_name = 'Antonello'");

			var statement = new DeleteStatement(tableName, expr);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);

			var count = result.GetValue(0, 0).AsBigInt();
			Assert.AreEqual(1L,  ((SqlNumber) count.Value).ToInt64());

			var table = Query.GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);
		}

		[Test]
		public void DeleteTwoRows() {
			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("last_name = 'Provenzano'");

			var statement = new DeleteStatement(tableName, expr);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);

			var count = result.GetValue(0, 0).AsBigInt();
			Assert.AreEqual(2L, ((SqlNumber)count.Value).ToInt64());

			var table = Query.GetTable(tableName);

			Assert.AreEqual(0, table.RowCount);
		}
	}
}
