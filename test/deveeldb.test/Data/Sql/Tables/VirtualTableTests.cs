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
using System.Threading.Tasks;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables.Model;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class VirtualTableTests {
		private ITable left;

		public VirtualTableTests() {
			var leftInfo = new TableInfo(ObjectName.Parse("tab1"));
			leftInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			leftInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));

			var temp = new TemporaryTable(leftInfo);
			temp.AddRow(new [] { SqlObject.Integer(23), SqlObject.Boolean(true) });
			temp.AddRow(new [] { SqlObject.Integer(54),SqlObject.Boolean(null) });

			left = temp;
		}

		[Fact]
		public void NewVirtualTableFromOneTableSource() {
			var table = new VirtualTable(ObjectName.Parse("#table#"), left, new long[]{1});

			Assert.Equal(1, table.RowCount);
			Assert.Equal(2, table.TableInfo.Columns.Count);
		}

		[Fact]
		public async Task GetLastValueOfOneTableSource() {
			var table = new VirtualTable(ObjectName.Parse("#table#"), left, new long[] { 1 });

			var value = await table.GetValueAsync(0, 1);

			Assert.NotNull(value);
			Assert.IsType<SqlBooleanType>(value.Type);
		}
	}
}