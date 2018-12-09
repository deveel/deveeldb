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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Serialization;
using Deveel.Data.Sql;

namespace Deveel.Data.Sql.Tables {
	class TestTable : IRootTable {
		private readonly IList<SqlObject[]> rows;
		private readonly TableInfo tableInfo;

		public TestTable(TableInfo tableInfo, IList<SqlObject[]> rows) {
			this.tableInfo = tableInfo;
			this.rows = rows;
		}

		IDbObjectInfo IDbObject.ObjectInfo => tableInfo;

		TableInfo ITable.TableInfo => tableInfo;

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			throw new NotSupportedException();
		}

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			return false;
		}

		public IEnumerator<Row> GetEnumerator() {
			return rows.Select((values, index) => new Row(this, index)).GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public long RowCount => rows.Count;

		public Task<SqlObject> GetValueAsync(long row, int column) {
			var objRow = rows[(int) row];
			return Task.FromResult(objRow[column]);
		}

		bool IEquatable<ITable>.Equals(ITable other) {
			return this == other;
		}

		public void Dispose() {
		}
	}
}