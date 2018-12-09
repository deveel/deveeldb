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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class LimitedTableTests {
		private ITable left;

		public LimitedTableTests() {
			var leftInfo = new TableInfo(ObjectName.Parse("tab1"));
			leftInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			leftInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));
			leftInfo.Columns.Add(new ColumnInfo("c", PrimitiveTypes.Double()));

			var temp = new TemporaryTable(leftInfo);
			temp.AddRow(new[] {SqlObject.Integer(23), SqlObject.Boolean(true), SqlObject.Double(5563.22)});
			temp.AddRow(new[] {SqlObject.Integer(54), SqlObject.Boolean(null), SqlObject.Double(921.001)});
			temp.AddRow(new[] {SqlObject.Integer(23), SqlObject.Boolean(true), SqlObject.Double(2010.221)});
			temp.BuildIndex();

			left = temp;
		}

		[Fact]
		public async Task LimitWithOffsetAndTotal() {
			var limited = new LimitedTable(left, 1, 1);

			Assert.Equal(1, limited.RowCount);
			Assert.Equal(3, limited.TableInfo.Columns.Count);

			var value1 = await limited.GetValueAsync(0, 0);
			var value2 = await limited.GetValueAsync(0, 1);

			Assert.Equal(SqlObject.Integer(54), value1);
			Assert.Equal(SqlObject.Boolean(null), value2);
		}

		[Fact]
		public async Task LimitWithTotal() {
			var limited = new LimitedTable(left, -1, 2);

			Assert.Equal(2, limited.RowCount);
			Assert.Equal(3, limited.TableInfo.Columns.Count);

			var value1 = await limited.GetValueAsync(0, 0);
			var value2 = await limited.GetValueAsync(0, 1);

			Assert.Equal(SqlObject.Integer(23), value1);
			Assert.Equal(SqlObject.Boolean(true), value2);
		}

		[Fact]
		public async Task EnumerateLimited() {
			var limited = new LimitedTable(left, -1, 2);

			Assert.Equal(2, limited.RowCount);
			Assert.Equal(3, limited.TableInfo.Columns.Count);

			var row1 = limited.First();

			var value1 = await row1.GetValueAsync("a");

			Assert.Equal(SqlObject.Integer(23), value1);
		}
	}
}
