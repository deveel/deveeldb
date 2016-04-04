using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class InsertSelectTests : ContextBasedTest {
		protected override void OnSetUp(string testName, IQuery query) {
			CreateTestTable(query);
		}

		private static void CreateTestTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime(), false);
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.Session.Access().CreateTable(tableInfo);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		protected override void OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");

			query.Access().DropAllTableConstraints(tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);
		}

		[Test]
		public void WithColumns() {
			var tableName = ObjectName.Parse("APP.test_table");
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT 'Antonello', 'Provenzano', NULL, NULL");
			var columns = new[] { "first_name", "last_name", "birth_date", "active" };

			var count = Query.InsertSelect(tableName, columns, query);

			Assert.AreEqual(1, count);

			var table = Query.Access().GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);
		}

		[Test]
		public void WithNoColumns() {
			var tableName = ObjectName.Parse("APP.test_table");
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT 3, 'Antonello', 'Provenzano', NULL, NULL");

			var count = Query.InsertSelect(tableName, query);

			Assert.AreEqual(1, count);

			var table = Query.Access().GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);

		}

	}
}
