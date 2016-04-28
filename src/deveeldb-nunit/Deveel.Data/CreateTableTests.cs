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

		protected override bool OnTearDown(string testName, IQuery query) {
			if (testName == "SimpleCreate" ||
			    testName == "WithColumnDefault") {
				var tableName = ObjectName.Parse("APP.test");
				query.Access().DropObject(DbObjectType.Table, tableName);
			} else {
				var tableName = ObjectName.Parse("APP.test_table");
				query.Access().DropAllTableConstraints(tableName);
				query.Access().DropObject(DbObjectType.Table, tableName);
			}

			return true;
		}

		[Test]
		public void SimpleCreate() {
			var tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()),
			};

			Query.CreateTable(tableName, columns);

			// TODO: Assert it exists and has the structure desired...
		}

		[Test]
		public void WithColumnDefault() {
			var tableName = ObjectName.Parse("APP.test");
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
	}
}
