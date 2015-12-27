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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class CreateViewStatementTests : ContextBasedTest {
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
		public void ExecuteSimpleCreateView() {
			const string sql = "CREATE VIEW text_view1 AS SELECT * FROM test_table WHERE a = 1";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateViewStatement>(statement);

			ITable result = null;
			Assert.DoesNotThrow(() => result = statement.Execute(Query));
			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}
	}
}
