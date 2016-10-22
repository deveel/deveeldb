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
using System.Data;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class DbDataReaderTests : ContextBasedTest {
		private IDbConnection connection;

		private void CreateTable(IQuery query) {
			query.Access().CreateTable(table => table
				.Named("APP.test_table")
				.WithColumn("a", PrimitiveTypes.Integer())
				.WithColumn("b", PrimitiveTypes.String()));
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
			connection = Database.CreateDbConnection(AdminUserName, AdminPassword);
		}

		private IDataReader CreateReader(string sql) {
			var command = connection.CreateCommand();
			command.CommandText = sql;
			return command.ExecuteReader();
		}

		[Test]
		public void ReadFirst() {
			var reader = CreateReader("SELECT * FROM APP.test_table");

			Assert.IsNotNull(reader);
			Assert.IsTrue(reader.Read());

			Assert.AreEqual(2, reader.FieldCount);

			var a = reader.GetInt32(0);
			var b = reader.GetString(1);

			Assert.AreEqual(0, a);
			Assert.AreEqual("b_0", b);
		}

		[Test]
		public void GetInt32() {
			var reader = CreateReader("SELECT * FROM APP.test_table");

			Assert.IsNotNull(reader);
			Assert.IsTrue(reader.Read());

			Assert.AreEqual(2, reader.FieldCount);

			var a = reader.GetInt32(0);
			Assert.AreEqual(0, a);
		}

		[Test]
		public void GetInt64() {
			var reader = CreateReader("SELECT * FROM APP.test_table");

			Assert.IsNotNull(reader);
			Assert.IsTrue(reader.Read());

			Assert.AreEqual(2, reader.FieldCount);

			var a = reader.GetInt64(0);
			Assert.AreEqual(0, a);
		}

		[Test]
		public void GetInt16() {
			var reader = CreateReader("SELECT * FROM APP.test_table");

			Assert.IsNotNull(reader);
			Assert.IsTrue(reader.Read());

			Assert.AreEqual(2, reader.FieldCount);

			var a = reader.GetInt16(0);
			Assert.AreEqual(0, a);
		}

		[Test]
		public void GetFloat() {
			var reader = CreateReader("SELECT * FROM APP.test_table");

			Assert.IsNotNull(reader);
			Assert.IsTrue(reader.Read());

			Assert.AreEqual(2, reader.FieldCount);

			var a = reader.GetFloat(0);
			Assert.AreEqual(0, a);
		}

		[Test]
		public void GetStringFromInteger() {
			var reader = CreateReader("SELECT * FROM APP.test_table");

			Assert.IsNotNull(reader);
			Assert.IsTrue(reader.Read());

			Assert.AreEqual(2, reader.FieldCount);

			var a = reader.GetString(0);
			Assert.AreEqual("0", a);
		}
	}
}