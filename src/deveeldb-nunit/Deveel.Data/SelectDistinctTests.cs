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
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectDistinctTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			var tableName = ObjectName.Parse("APP.persons");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("age", PrimitiveTypes.Integer());

			Query.Access().CreateObject(tableInfo);

			var table = Query.Access().GetMutableTable(tableName);

			var row = table.NewRow();
			row.SetValue(0, "Antonello");
			row.SetValue(1, "Provenzano");
			row.SetValue(2, 33);
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue(0, "Mark");
			row.SetValue(1, "Smith");
			row.SetValue(2, 42);
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue(0, "Maggie");
			row.SetValue(1, "Smith");
			row.SetValue(2, 65);
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue(0, "Antonello");
			row.SetValue(1, "Capone");
			row.SetValue(2, 33);
			table.AddRow(row);
		}

		private ITable Execute(string s) {
			var query = (SqlQueryExpression) SqlExpression.Parse(s);
			Assert.IsTrue(query.Distinct);
			var result = Query.Select(query);
			result.GetEnumerator().MoveNext();
			return result.Source;
		}

		[Test]
		public void DistinctMulti_OneColumn() {
			var result = Execute("SELECT DISTINCT last_name FROM persons");

			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.RowCount);
		}

		[Test]
		public void DistinctMulti_TwoColumns() {
			var result = Execute("SELECT DISTINCT first_name, age FROM persons");

			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.RowCount);
		}
	}
}
