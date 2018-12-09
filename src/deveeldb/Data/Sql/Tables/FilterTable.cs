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

namespace Deveel.Data.Sql.Tables {
	public class FilterTable : TableBase {
		public FilterTable(ITable parent) {
			Parent = parent;
		}

		protected ITable Parent { get; }

		public override TableInfo TableInfo => Parent.TableInfo;
		
		public override long RowCount => Parent.RowCount;

		public override Task<SqlObject> GetValueAsync(long row, int column) {
			return Parent.GetValueAsync(row, column);
		}

		public override IEnumerator<Row> GetEnumerator()
			=> Parent.GetEnumerator();

		protected override IEnumerable<long> ResolveRows(int column, IEnumerable<long> rows, ITable ancestor) {
			if (ancestor == this || ancestor == Parent)
				return rows;

			return  Parent.ResolveRows(column, rows, ancestor);
		}

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			return Parent.GetRawTableInfo(rootInfo);
		}
	}
}