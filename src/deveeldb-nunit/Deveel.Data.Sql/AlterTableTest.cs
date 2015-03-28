// 
//  Copyright 2011 Deveel
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

using Deveel.Data.Client;
using Deveel.Data.DbSystem;

using NUnit.Framework;

using SysDataTable = System.Data.DataTable;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class AlterTableTest : SqlTestBase {
		[Test]
		public void AddColumn() {
			ExecuteNonQuery("CREATE TABLE Test (id IDENTITY, name VARCHAR)", true);
			ExecuteNonQuery("ALTER TABLE Test ADD COLUMN description  VARCHAR(255);", true);

			IDatabaseConnection connection = CreateDatabaseConnection();
			Table table = connection.GetTable("Test");
			Assert.IsNotNull(table);

			int index = table.TableInfo.FindColumnName("description");
			Assert.AreNotEqual(-1, index);
			Assert.AreEqual("description", table.TableInfo[index].Name);
			Assert.AreEqual(SqlType.VarChar, table.TableInfo[index].SqlType);
			Assert.AreEqual(255, table.TableInfo[index].Size);

			ExecuteNonQuery("DROP TABLE Test",true);
		}
		
		[Test(Description = "Adds a column that already was defined in the table.")]
		public void AddExistingColumn() {
			Assert.Throws<DeveelDbException>(delegate { ExecuteNonQuery("ALTER TABLE Person ADD COLUMN name VARCHAR(30);"); });
		}

		[Test]
		public void DropColumn() {
			ExecuteNonQuery("CREATE TABLE Test (id INT, name VARCHAR)", true);
			ExecuteNonQuery("ALTER TABLE Test DROP COLUMN name;", true);

			IDatabaseConnection connection = CreateDatabaseConnection();
			Table table = connection.GetTable("Test");
			Assert.IsNotNull(table);
			Assert.IsTrue(table.TableInfo.FindColumnName("name") == -1);

			ExecuteNonQuery("DROP TABLE Test", true);
		}

		[Test]
		public void DropNonExistingColumn() {
			ExecuteNonQuery("CREATE TABLE Test (id INT, name VARCHAR)", true);
			ExecuteNonQuery("ALTER TABLE Test DROP COLUMN name;", true);

			var count = ExecuteNonQuery("ALTER TABLE Test DROP COLUMN description;");
			Assert.AreEqual(0, count);

			ExecuteNonQuery("DROP TABLE Test", true);
		}

		[Test]
		public void AddForeignKeyConstraint() {
			ExecuteNonQuery("CREATE TABLE test_table_1 (prop1 NUMERIC, prop2 VARCHAR);", true);
			ExecuteNonQuery("CREATE TABLE test_table_2 (prop1 NUMERIC, prop2 VARCHAR);", true);
			ExecuteNonQuery("ALTER TABLE test_table_1 ADD CONSTRAINT fk_test_table FOREIGN KEY (prop1) REFERENCES test_table_2 (prop1);", true);

			IDatabaseConnection connection = CreateDatabaseConnection();
			DataConstraintInfo[] fkeys = connection.QueryTableForeignKeyReferences(new TableName("APP", "test_table_1"));

			Assert.IsNotNull(fkeys);
			Assert.AreEqual(1, fkeys.Length);
			Assert.AreEqual("fk_test_table", fkeys[0].Name);
			Assert.AreEqual("prop1", fkeys[0].Columns[0]);

			ExecuteNonQuery("DROP TABLE test_table_1", true);
			ExecuteNonQuery("DROP TABLE test_table_2", true);
		}

		[Test]
		public void DropForeignKeyConstraint() {
			ExecuteNonQuery("CREATE TABLE test_table_1 (prop1 NUMERIC, prop2 VARCHAR);", true);
			ExecuteNonQuery("CREATE TABLE test_table_2 (prop1 NUMERIC, prop2 VARCHAR);", true);
			ExecuteNonQuery("ALTER TABLE test_table_1 ADD CONSTRAINT fk_test_table FOREIGN KEY (prop1) REFERENCES test_table_2 (prop1);", true);

			ExecuteNonQuery("ALTER TABLE test_table_1 DROP CONSTRAINT fk_test_table;", true);

			IDatabaseConnection connection = CreateDatabaseConnection();
			DataConstraintInfo[] fkeys = connection.QueryTableForeignKeyReferences(new TableName("APP", "test_table_1"));
			Assert.IsEmpty(fkeys);

			ExecuteNonQuery("DROP TABLE test_table_1", true);
			ExecuteNonQuery("DROP TABLE test_table_2", true);
		}

		[Test]
		public void SetColumnDefault() {
			ExecuteNonQuery("CREATE TABLE test_table_1 (prop1 NUMERIC, prop2 VARCHAR);", true);
			ExecuteNonQuery("ALTER TABLE test_table_1 ALTER prop1 SET prop1 = -1;", true);

			IDatabaseConnection connection = CreateDatabaseConnection();
			Table table = connection.GetTable("test_table_1");
			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.TableInfo.ColumnCount);
			Assert.AreEqual("prop1", table.TableInfo[0].Name);
			Assert.AreEqual("prop1 = -1", table.TableInfo[0].GetDefaultExpressionString());

			ExecuteNonQuery("DROP TABLE test_table_1", true);
		}

		[Test]
		public void DropColumnDefault() {
			ExecuteNonQuery("CREATE TABLE test_table_1 (prop1 NUMERIC, prop2 VARCHAR);", true);
			ExecuteNonQuery("ALTER TABLE test_table_1 ALTER prop1 SET prop1 = -1;", true);

			ExecuteNonQuery("ALTER TABLE test_table_1 ALTER prop1 DROP DEFAULT;", true);

			IDatabaseConnection connection = CreateDatabaseConnection();
			Table table = connection.GetTable("test_table_1");
			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.TableInfo.ColumnCount);
			Assert.AreEqual("prop1", table.TableInfo[0].Name);
			Assert.IsNullOrEmpty(table.TableInfo[0].GetDefaultExpressionString());

			ExecuteNonQuery("DROP TABLE test_table_1", true);
		}

		[Test]
		public void DropPrimaryKeyConstraint() {
			ExecuteNonQuery("CREATE TABLE test_table_1 (prop1 NUMERIC, prop2 VARCHAR, PRIMARY KEY(prop1));", true);
			ExecuteNonQuery("ALTER TABLE test_table_1 DROP PRIMARY KEY", true);

			IDatabaseConnection connection = CreateDatabaseConnection();
			Table table = connection.GetTable("test_table_1");
			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.TableInfo.ColumnCount);

			DataConstraintInfo pkey = connection.QueryTablePrimaryKeyGroup(new TableName("APP", "test_table_1"));

			Assert.IsNull(pkey);

			ExecuteNonQuery("DROP TABLE test_table_1", true);
		}
	}
}