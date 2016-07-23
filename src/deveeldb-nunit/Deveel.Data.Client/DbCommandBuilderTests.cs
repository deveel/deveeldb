using System;
using System.Data;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public class DbCommandBuilderTests : ContextBasedTest {
		private DeveelDbConnection connection;

		private void CreateTable(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			query.Access().CreateTable(tableInfo);
		}

		private void AddTestData(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var table = query.Access().GetMutableTable(tableName);

			for (int i = 0; i < 23; i++) {
				var row = table.NewRow();

				row["a"] = Field.Integer(i);
				row["b"] = Field.String(String.Format("b_{0}", i));

				table.AddRow(row);
			}
		}

		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTable(query);
			AddTestData(query);
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		protected override void OnAfterSetup(string testName) {
			connection = (DeveelDbConnection) Database.CreateDbConnection(AdminUserName, AdminPassword);
		}

		[Test]
		public void BuildInsert() {
			var adapter = new DeveelDbDataAdapter(connection, "SELECT * FROM test_table");
			var builder = new DeveelDbCommandBuilder(adapter);

			var command = builder.GetInsertCommand();

			Assert.IsNotNull(command);
		}
	}
}
