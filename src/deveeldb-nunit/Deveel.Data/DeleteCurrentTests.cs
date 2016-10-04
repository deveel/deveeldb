using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DeleteCurrentTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTable(query);
			InsertTestData(query);
			return true;
		}

		private void CreateTable(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn(new ColumnInfo("id", PrimitiveTypes.Integer()) {
				DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY", new SqlExpression[] {
					SqlExpression.Constant(tableName.FullName)
				})
			});
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);
			query.Access().CreateTable(tableInfo);
			query.Access().AddPrimaryKey(tableName, "id", "PK_TEST_TABLE");
		}

		private void InsertTestData(IQuery query) {
			var table = query.Access().GetMutableTable(ObjectName.Parse("APP.test_table"));
			var row = table.NewRow();
			row["a"] = Field.Integer(33);
			row["b"] = Field.String("test string");
			row.SetDefault(query);
			table.AddRow(row);

			row = table.NewRow();
			row["a"] = Field.Integer(0);
			row["b"] = Field.String("Another string to test");
			row.SetDefault(query);
			table.AddRow(row);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropObject(DbObjectType.Table, ObjectName.Parse("APP.test_table"));
			return true;
		}

		protected override void OnAfterSetup(string testName) {
			DeclareCursor(AdminQuery);
			OpenAndAdvanceCursor(AdminQuery);
		}

		private static void DeclareCursor(IQuery query) {
			var queryExp1 = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table");
			query.Context.DeclareCursor("c1", queryExp1);
		}

		private static void OpenAndAdvanceCursor(IQuery query) {
			var cursor = (Cursor) query.Access().GetObject(DbObjectType.Cursor, new ObjectName("c1"));
			cursor.Open(query);
			cursor.Fetch(FetchDirection.Next);
		}

		[Test]
		public void FirstElement() {
			var tableName = ObjectName.Parse("APP.test_table");

			AdminQuery.DeleteCurrent(tableName, "c1");

			var table = AdminQuery.Access().GetTable(ObjectName.Parse("APP.test_table"));

			Assert.AreEqual(1, table.RowCount);
		}
	}
}
