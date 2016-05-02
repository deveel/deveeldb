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

using Deveel.Data.Mapping;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture(StorageType.InMemory)]
	[TestFixture(StorageType.SingleFile)]
	[TestFixture(Data.StorageType.JournaledFile)]
	public sealed class CreateTableTests : ContextBasedTest {
		public CreateTableTests(StorageType storageType)
			: base(storageType) {
		}

		private ObjectName tableName;

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropAllTableConstraints(tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		[Test]
		public void SimpleCreate() {
			tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()),
			};

			Query.CreateTable(tableName, columns);

			// TODO: Assert it exists and has the structure desired...
		}

		[Test]
		public void WithColumnDefault() {
			tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()) {
					DefaultExpression = SqlExpression.Parse("((67 * 90) + 22)")
				},
				new SqlTableColumn("date", PrimitiveTypes.TimeStamp()) {
					DefaultExpression = SqlExpression.Parse("GetDate()")
				}
			};

			Query.CreateTable(tableName, columns);

			// TODO: Assert it exists and has the structure desired...
		}

		[Test]
		public void WithColumnIndex() {
			tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()) {
					IndexType = "BLIST"
				},
				new SqlTableColumn("date", PrimitiveTypes.TimeStamp()) {
					DefaultExpression = SqlExpression.Parse("GetDate()")
				}
			};

			Query.CreateTable(tableName, columns);
		}

		[Test]
		public void FromMap() {
			Query.CreateTable<TestClass>();

			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = Query.Access().GetTableInfo(tableName);

			Assert.IsNotNull(tableInfo);
			Assert.AreEqual(3, tableInfo.ColumnCount);

			var idColumn = tableInfo[0];
			Assert.IsNotNull(idColumn);
			Assert.AreEqual("Id", idColumn.ColumnName);
			Assert.IsNotNull(idColumn.DefaultExpression);

			var primaryKey = Query.Access().QueryTablePrimaryKey(tableName);
			Assert.IsNotNull(primaryKey);
		}

		[TableName("test_table")]
		class TestClass {
			[Column, Identity]
			public int Id { get; set; }

			[Column]
			public string FirstName { get; set; }

			[Column(Null = true)]
			public string LastName { get; set; }

			[Mapping.Ignore]
			public int Dummy { get; set; }
		}
	}
}
