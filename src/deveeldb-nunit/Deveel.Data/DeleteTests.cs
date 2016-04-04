using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DeleteTests : ContextBasedTest {
		protected override void OnSetUp(string testName, IQuery query) {
			CreateTestTable(query);
			InsertTestData(query);
		}

		private void CreateTestTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.Session.Access().CreateTable(tableInfo);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		private void InsertTestData(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");

			var table = query.Access().GetMutableTable(tableName);
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

		protected override void OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropAllTableConstraints(tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);
		}

		[Test]
		public void OnlyOneRow() {
			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("first_name = 'Antonello'");

			var count = Query.Delete(tableName, expr);
			Assert.AreEqual(1, count);

			var table = Query.Access().GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);
		}

		[Test]
		public void TwoRows() {
			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("last_name = 'Provenzano'");

			var count = Query.Delete(tableName, expr);
			Assert.AreEqual(2, count);

			var table = Query.Access().GetTable(tableName);

			Assert.AreEqual(0, table.RowCount);
		}

	}
}
