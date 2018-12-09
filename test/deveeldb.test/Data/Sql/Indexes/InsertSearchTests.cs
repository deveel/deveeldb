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

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Indexes {
	public class InsertSearchTests : IDisposable {
		private TemporaryTable left;

		public InsertSearchTests() {
			var leftInfo = new TableInfo(ObjectName.Parse("tab1"));
			leftInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			leftInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));
			leftInfo.Columns.Add(new ColumnInfo("c", PrimitiveTypes.Double()));

			left = new TemporaryTable(leftInfo);
			left.AddRow(new[] { SqlObject.Integer(23), SqlObject.Boolean(true), SqlObject.Double(5563.22) });
			left.AddRow(new[] { SqlObject.Integer(54), SqlObject.Boolean(null), SqlObject.Double(921.001) });
			left.AddRow(new[] { SqlObject.Integer(23), SqlObject.Boolean(true), SqlObject.Double(2010.221) });
		}

		[Fact]
		public void SelectAll_SingleColumn_UniqueValues() {
			var index = CreateFullIndex("c");

			var result = index.SelectAll();

			Assert.NotNull(result);
			Assert.NotEmpty(result);

			var list = result.ToBigArray();

			Assert.Equal(3, list.Length);
			Assert.Equal(1, list[0]);
			Assert.Equal(2, list[1]);
			Assert.Equal(0, list[2]);
		}

		[Fact]
		public void SelectAll_SingleColumn() {
			var index = CreateFullIndex("a");

			var result = index.SelectAll();

			Assert.NotNull(result);
			Assert.NotEmpty(result);

			var list = result.ToBigArray();

			Assert.Equal(3, list.Length);
			Assert.Equal(0, list[0]);
			Assert.Equal(2, list[1]);
			Assert.Equal(1, list[2]);
		}

		[Fact]
		public void SelectAll_MultiColumn() {
			var index = CreateFullIndex("a", "c");

			var result = index.SelectAll();

			Assert.NotNull(result);
			Assert.NotEmpty(result);

			var list = result.ToBigArray();

			Assert.Equal(3, list.Length);
		}

		[Fact]
		public void SelectGreater() {
			var index = CreateFullIndex("a");

			var result = index.SelectGreater(new[] {SqlObject.Integer(24)});

			Assert.NotEmpty(result);
			Assert.Single(result);

			Assert.Equal(1, result.ElementAt(0));
		}

		[Fact]
		public void SelectLess() {
			var index = CreateFullIndex("a");

			var result = index.SelectLess(new[] { SqlObject.Integer(24) });

			Assert.NotEmpty(result);
			Assert.Equal(2, result.Count());

			Assert.Equal(0, result.ElementAt(0));
		}

		[Fact]
		public void SelectGreaterOrEqual() {
			var index = CreateFullIndex("a");

			var result = index.SelectGreaterOrEqual(new[] { SqlObject.Integer(24) });

			Assert.NotEmpty(result);
			Assert.Equal(1, result.Count());

			Assert.Equal(1, result.ElementAt(0));
		}

		[Fact]
		public void SelectLessOrEqual() {
			var index = CreateFullIndex("a");

			var result = index.SelectLessOrEqual(new[] { SqlObject.Integer(24) });

			Assert.NotEmpty(result);
			Assert.Equal(2, result.Count());

			Assert.Equal(0, result.ElementAt(0));
		}

		[Fact]
		public void CreateSubset() {
			var index = CreateFullIndex("a");

			var subset = index.Subset(left, 0);

			Assert.NotNull(subset);

			var result = subset.SelectAll();
			Assert.NotEmpty(result);
			Assert.Equal(3, result.Count());
		}

		private TableIndex CreateFullIndex(params string[] columnNames) {
			var indexInfo = new IndexInfo(ObjectName.Parse("sys.idx1"), left.TableInfo.TableName, columnNames);
			var index = new InsertSearchIndex(indexInfo, left);

			foreach (var row in left) {
				index.Insert(row.Number);
			}

			return index;
		}

		public void Dispose() {
			left = null;
		}
	}
}