// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Data.Common;
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class DbCommandTest : ContextBasedTest {
		private DeveelDbConnection connection;

		protected override bool OnSetUp(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.VarChar());
			tableInfo.AddColumn("age", PrimitiveTypes.TinyInt());

			if (testName.EndsWith("Lob"))
				tableInfo.AddColumn("bio", PrimitiveTypes.Clob(6030));

			query.Access().CreateObject(tableInfo);

			AddTestData(testName, query);

			return true;
		}

		private void AddTestData(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var table = query.Access().GetMutableTable(tableName);

			var row = table.NewRow();
			row["id"] = Field.Integer(1);
			row["name"] = Field.String("Antonello Provenzano");
			row["age"] = Field.Integer(35);

			if (testName.EndsWith("Lob")) {
				row["bio"] = Field.Clob(SqlLongString.Unicode(query, "Some bio about Antonello that is stored in the database"));
			}

			table.AddRow(row);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		protected override void OnAfterSetup(string testName) {
			var connString = new DeveelDbConnectionStringBuilder {
				UserName = AdminUserName,
				Password = AdminPassword,
				DataSource = "memory",
				Database = DatabaseName,
				Schema = "APP",
				Create = true
			};

			connection = (DeveelDbConnection) Database.CreateDbConnection(AdminUserName, AdminPassword);
		}

		[Test]
		public void CreateOnNotOpenConnection() {
			var command = connection.CreateCommand();

			Assert.IsNotNull(command);
		}

		[Test]
		public void CreateEmpty() {
			DbCommand command = null;
			Assert.DoesNotThrow(() => command = new DeveelDbCommand());
			Assert.IsNotNull(command);
			Assert.IsNull(command.Connection);
		}

		[Test]
		public void CreateWithConnectionArgument() {
			DbCommand command = null;
			Assert.DoesNotThrow(() => command = new DeveelDbCommand(connection));
			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);
		}

		[Test]
		public void AttachConnectionToCommand() {
			DbCommand command = null;
			Assert.DoesNotThrow(() => command = new DeveelDbCommand());
			Assert.IsNotNull(command);
			Assert.DoesNotThrow(() => command.Connection = connection);
		}

		[Test]
		public void AssignSimple() {
			DbCommand command = null;

			Assert.DoesNotThrow(() => command = connection.CreateCommand());
			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "a := 22";
			Assert.DoesNotThrow(() => command.ExecuteNonQuery());
		}

		[Test]
		public void AssignWithParameters() {
			var command = connection.CreateCommand();

			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "a := ?";
			command.Parameters.Add(22);

			Assert.DoesNotThrow(() => command.ExecuteNonQuery());
		}

		[Test]
		public void AssignWithSelectScalar() {
			var command = connection.CreateCommand();

			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "a := ?";
			command.Parameters.Add(22);

			var result = command.ExecuteScalar();
			
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<int>(result);
			Assert.AreEqual(22, result);
		}

		[Test]
		public void SelectTables() {
			var command = connection.CreateCommand();

			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "SELECT * FROM INFORMATION_SCHEMA.Tables";

			var reader = command.ExecuteReader();

			Assert.IsNotNull(reader);
			Assert.IsTrue(reader.Read());
			Assert.AreEqual(10, reader.FieldCount);

			var col1 = reader.GetName(0);
			Assert.AreEqual("INFORMATION_SCHEMA.tables.TABLE_CATALOG", col1);
			var col2 = reader.GetName(1);
			Assert.AreEqual("INFORMATION_SCHEMA.tables.TABLE_SCHEMA", col2);
			var col3 = reader.GetName(2);
			Assert.AreEqual("INFORMATION_SCHEMA.tables.TABLE_NAME", col3);

			var value = reader.GetValue(2);
		}

		[Test]
		public void SelectWithLob() {
			var command = connection.CreateCommand();

			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "SELECT * FROM test_table";

			var reader = command.ExecuteReader();

			Assert.IsNotNull(reader);
			Assert.IsTrue(reader.Read());
			Assert.AreEqual(4, reader.FieldCount);

			var col1Name = reader.GetName(0);
			var col1Value = reader.GetValue(0);
			Assert.AreEqual("APP.test_table.id", col1Name);
			Assert.AreEqual(1, col1Value);

			var col2Name = reader.GetName(1);
			var col2Value = reader.GetString(1);
			Assert.AreEqual("APP.test_table.name", col2Name);
			Assert.AreEqual("Antonello Provenzano", col2Value);

			var col4Name = reader.GetName(3);
			var col4Value = new byte[6030];
			var col4Count = reader.GetBytes(3, 0, col4Value, 0, col4Value.Length);
			Assert.AreEqual("APP.test_table.bio", col4Name);
			Assert.AreEqual(110, col4Count);
			Assert.IsNotNull(col4Value);

			var col4StringValue = Encoding.Unicode.GetString(col4Value);

			Assert.IsNotNullOrEmpty(col4StringValue);
		}
	}
}
