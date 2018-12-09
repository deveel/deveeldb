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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Indexes;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class FilterTableTests : IDisposable {
		private TemporaryTable left;

		public FilterTableTests() {
			var leftInfo = new TableInfo(ObjectName.Parse("tab1"));
			leftInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			leftInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));
			left = new TemporaryTable(leftInfo);

			left.AddRow(new[] { SqlObject.Integer(23), SqlObject.Boolean(true) });
			left.AddRow(new[] { SqlObject.Integer(54), SqlObject.Boolean(null) });

			left.BuildIndex();
		}

		[Fact]
		public void GetSubsetIndex() {
			var table = new FilterTable(left);
			var index = table.GetColumnIndex(0);

			Assert.NotNull(index);

			var rows = index.SelectGreater(new IndexKey(SqlObject.Integer(24)));

			Assert.Single(rows);
		}

		public void Dispose() {
			
		}
	}
}