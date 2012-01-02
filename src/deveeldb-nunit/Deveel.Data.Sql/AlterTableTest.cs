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

using NUnit.Framework;

using SysDataTable = System.Data.DataTable;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class AlterTableTest : TestBase {
		protected override bool RequiresSchema {
			get { return true; }
		}

		private bool HasColumn(string schemaName, string tableName, string columnName) {
			SysDataTable table = Connection.GetSchema(DeveelDbMetadataSchemaNames.Columns, new string[] { null, schemaName, tableName, null});
			foreach (System.Data.DataRow dataRow in table.Rows) {
				if (dataRow["COLUMN_NAME"].Equals(columnName))
					return true;
			}

			return false;
		}

		[Test]
		public void AddColumn() {
			ExecuteNonQuery("ALTER TABLE Person ADD COLUMN description  VARCHAR(255);");
			Assert.IsTrue(HasColumn("APP", "Person", "description"));
		}
		
		[Test(Description = "Adds a column that already was defined in the table.")]
		public void AddExistingColumn() {
			Assert.Throws<DatabaseException>(delegate { ExecuteNonQuery("ALTER TABLE Person ADD COLUMN name VARCHAR(30);"); });
		}

		[Test]
		public void DropColumn() {
			ExecuteNonQuery("ALTER TABLE Person DROP COLUMN name;");
			Assert.IsFalse(HasColumn("APP", "Person", "name"));
		}

		[Test]
		public void DropNonExistingColumn() {
			Assert.Throws<DatabaseException>(delegate { ExecuteNonQuery("ALTER TABLE Person DROP COLUMN description;"); });
		}

		[Test]
		public void AddForeignKeyConstraint() {
			ExecuteNonQuery("ALTER TABLE Person ADD FOREIGN KEY (name) REFERENCES ListensTo (person_name);");
		}

		[Test]
		public void DropConstraint() {
			Assert.Inconclusive();
		}

		[Test]
		public void SetColumnDefault() {
			Assert.Inconclusive();
		}

		[Test]
		public void DropColumnDefault() {
			Assert.Inconclusive();
		}

		[Test]
		public void DropPrimaryKeyConstraint() {
			Assert.Inconclusive();
		}
	}
}