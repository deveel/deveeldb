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

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Indexes {
	public class TableIndexTests {
		private ITable table;

		public TableIndexTests() {
			var tableInfo = new TableInfo(ObjectName.Parse("test_table1"));
			tableInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.BigInt()));
			tableInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.String()));

			var temp = new TemporaryTable(tableInfo);
			temp.AddRow(new []{SqlObject.BigInt(22), SqlObject.String("o102") });
			temp.AddRow(new []{SqlObject.BigInt(103), SqlObject.String("t-23") });
			temp.AddRow(new[] {SqlObject.BigInt(104), SqlObject.String("test22")});
			temp.BuildIndex();

			table = temp;
		}

		[Fact]
		public void SelectFirst() {
			var index = table.GetIndex(new[] {0});

			var result = index.SelectFirst().ToBigArray();

			Assert.NotEmpty(result);
			Assert.Single(result);

			Assert.Equal(0, result[0]);
		}

		[Fact]
		public void SelectLast() {
			var index = table.GetIndex(new[] {0});

			var result = index.SelectLast().ToBigArray();

			Assert.NotEmpty(result);
			Assert.Single(result);

			Assert.Equal(table.RowCount -1, result[0]);
		}

		[Fact]
		public void SelectGreaterThan() {
			var index = table.GetIndex(new[] {0});

			var result = index.SelectGreater(SqlObject.BigInt(100)).ToBigArray();

			Assert.NotEmpty(result);
			Assert.Equal(2, result.Length);

			Assert.Equal(1, result[0]);
			Assert.Equal(2, result[1]);
		}

		[Fact]
		public void SelectLessThan() {
			var index = table.GetIndex(new[] {0});

			var result = index.SelectLess(SqlObject.BigInt(100)).ToBigArray();

			Assert.NotEmpty(result);
			Assert.Single(result);

			Assert.Equal(0, result[0]);
		}
	}
}