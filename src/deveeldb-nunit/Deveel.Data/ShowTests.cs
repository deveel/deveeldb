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
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class ShowTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var tableName = ObjectName.Parse("SYSTEM.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			query.Access().CreateObject(tableInfo);
			query.Access().GrantOnTable(tableName, User.PublicName, PrivilegeSets.TableAll);

			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("SYSTEM.test_table");
			query.Access().RevokeAllGrantsOn(DbObjectType.Table, tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		[Test]
		public void ShowSchema() {
			var result = AdminQuery.ShowSchema();

			Assert.IsNotNull(result);

			Row row = null;
			Assert.DoesNotThrow(() => row = result.ElementAt(0));
			Assert.IsNotNull(row);

			var schemaName = row.GetValue(0).Value.ToString();
			var schemaType = row.GetValue(1).Value.ToString();

			Assert.AreEqual("APP", schemaName);
			Assert.AreEqual("DEFAULT", schemaType);

			Assert.DoesNotThrow(() => row = result.ElementAt(1));
			Assert.IsNotNull(row);

			schemaName = row.GetValue(0).Value.ToString();
			schemaType = row.GetValue(1).Value.ToString();

			Assert.AreEqual("INFORMATION_SCHEMA", schemaName);
			Assert.AreEqual("SYSTEM", schemaType);
		}

		[Test]
		public void ShowTables() {
			var result = AdminQuery.ShowTables();

			Assert.IsNotNull(result);

			// TODO: There's probably an error on the views to select
			//        the tables the current user has access to... so for the moment
			//        just be happy it doesn't throw an error: we will come back later
		}

		[Test]
		public void ShowProduct() {
			var result = AdminQuery.ShowProduct();

			Assert.IsNotNull(result);

			// TODO: the product information come from the variables table,
			//        that is not yet finalized: so the execution succeeds but
			//        no data are retrieved. come back later when database vars
			//        are implemented
		}
	}
}
