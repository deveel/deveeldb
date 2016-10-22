using System;
using System.Data;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class NonQueryTests : ContextBasedTest {
		private IDbConnection connection;

		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateTable(table => table
				.Named("APP.test_table")
				.WithColumn("a", PrimitiveTypes.Integer())
				.WithColumn("b", PrimitiveTypes.String()));

			if (testName.StartsWith("Update") ||
			    testName.StartsWith("Delete"))
				AddTestData(query);

			return true;
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

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		protected override void OnAfterSetup(string testName) {
			var config = new Configuration.Configuration();
			config.SetValue("connection.userName", AdminUserName);
			config.SetValue("connection.password", AdminPassword);
			config.SetValue("connection.parameterStyle", QueryParameterStyle.Named);
			connection = Database.CreateDbConnection(config);
		}

		[Test]
		public void InsertInto() {
			var command = connection.CreateCommand();
			command.CommandText = "INSERT INTO test_table (a, b) VALUES (:a, :b)";
			command.Parameters.Add(new DeveelDbParameter(":a", 88));
			command.Parameters.Add(new DeveelDbParameter(":b", "b_88"));

			var result = command.ExecuteNonQuery();

			Assert.AreEqual(1, result);
		}
	}
}
