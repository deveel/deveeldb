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

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class CursorTests : ContextBasedTest {
		protected override ISession CreateAdminSession(IDatabase database) {
			using (var session = base.CreateAdminSession(database)) {
				using (var query = session.CreateQuery()) {
					var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
					tableInfo.AddColumn("a", PrimitiveTypes.Integer());
					tableInfo.AddColumn("b", PrimitiveTypes.String(), false);

					query.CreateTable(tableInfo, false, false);
					query.Commit();
				}
			}

			return base.CreateAdminSession(database);
		}

		[Test]
		public void DeclareInsensitiveCursor() {
			var query = new SqlQueryExpression(new []{new SelectColumn(SqlExpression.Constant("*")) });
			query.FromClause = new FromClause();
			query.FromClause.AddTable("test_table");

			Assert.DoesNotThrow(() => Query.DeclareInsensitiveCursor("c1", query));

			var exists = Query.ObjectExists(DbObjectType.Cursor, new ObjectName("c1"));
			Assert.IsTrue(exists);
		}
	}
}
