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

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class MutableTableTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			var table = CreateTable();

			if (testName != "InsertInto") {
				InsertIntoTable(table);
			}
		}

		private void InsertIntoTable(IMutableTable table) {
			var row = table.NewRow();
			row.SetValue(0,  "Antonello Provenzano");
			row.SetValue(1, 33);
			row.SetValue(2, 0);
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue(0, "Maart Roosmaa");
			row.SetValue(1, 28);
			row.SetValue(2, 5);
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue(0, "Rezaul Horaque");
			row.SetValue(1, 27);
			row.SetValue(2, 2);
			table.AddRow(row);
		}

		private IMutableTable CreateTable() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("name", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("age", PrimitiveTypes.Integer());
			tableInfo.AddColumn("order", PrimitiveTypes.Integer());

			Query.CreateTable(tableInfo);
			return Query.GetMutableTable(tableName);
		}
	}
}
