// 
//  Copyright 2010-2018 Deveel
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
//

using System;
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class TemporaryTableTests {
		[Fact]
		public static async Task CreateAndAddRows() {
			var tableInfo = new TableInfo(new ObjectName("table1"));
			tableInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			tableInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.VarChar(22)));

			var table = new TemporaryTable(tableInfo);
			table.AddRow(new SqlObject[] {
				SqlObject.Integer(22),
				SqlObject.String(new SqlString("test")), 
			});

			Assert.Equal(1, table.RowCount);

			var value1 = await table.GetValueAsync(0, 0);
			var value2 = await table.GetValueAsync(0, 1);

			Assert.Equal(SqlObject.Integer(22), value1);
			Assert.Equal(SqlObject.String(new SqlString("test")), value2);
		}

		[Fact]
		public static void EnuemrateRows() {
			var tableInfo = new TableInfo(new ObjectName("table1"));
			tableInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			tableInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.VarChar(22)));

			var table = new TemporaryTable(tableInfo);
			table.AddRow(new SqlObject[] {
				SqlObject.Integer(22),
				SqlObject.String(new SqlString("test")),
			});
			table.AddRow(new [] {
				SqlObject.Integer(15002933),
				SqlObject.String(new SqlString("test2")), 
			});

			Assert.Equal(2, table.Count());

			var row1 = table.ElementAt(0);
			var row2 = table.ElementAt(1);

			Assert.Equal(SqlObject.Integer(22), row1["a"]);
			Assert.Equal(SqlObject.String(new SqlString("test")), row1["b"]);
			Assert.Equal(SqlObject.Integer(15002933), row2["a"]);
		}

		[Fact]
		public static async Task SetRowValues() {
			var tableInfo = new TableInfo(new ObjectName("table1"));
			tableInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			tableInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.VarChar(22)));

			var table = new TemporaryTable(tableInfo);

			var row = table.NewRow();
			table.SetValue(row, 0, SqlObject.Integer(45));
			table.SetValue(row, 1, SqlObject.String(new SqlString("992")));

			Assert.Equal(1, table.RowCount);

			var value1 = await table.GetValueAsync(0, 0);
			Assert.Equal(SqlObject.Integer(45), value1);
		}
	}
}