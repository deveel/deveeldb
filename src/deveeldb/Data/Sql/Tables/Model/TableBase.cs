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
using System.Threading.Tasks;

using Deveel.Data.Sql.Indexes;

namespace Deveel.Data.Sql.Tables.Model {
	public abstract class TableBase : IVirtualTable {
		~TableBase() {
			Dispose(false);
		}

		public abstract TableInfo TableInfo { get; }

		IDbObjectInfo IDbObject.ObjectInfo => TableInfo;

		public abstract long RowCount { get; }

		public abstract IEnumerator<Row> GetEnumerator();

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		IEnumerable<long> IVirtualTable.ResolveRows(int column, IEnumerable<long> rows, ITable ancestor) {
			return ResolveRows(column, rows, ancestor);
		}

		protected abstract IEnumerable<long> ResolveRows(int column, IEnumerable<long> rows, ITable ancestor);

		protected abstract RawTableInfo GetRawTableInfo(RawTableInfo rootInfo);

		RawTableInfo IVirtualTable.GetRawTableInfo(RawTableInfo rootInfo) {
			return GetRawTableInfo(rootInfo);
		}

		public abstract Task<SqlObject> GetValueAsync(long row, int column);

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
		}

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			throw new NotSupportedException();
		}

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			return false;
		}

		TableIndex ITable.GetIndex(int[] columns) {
			if (columns.Length > 1)
				throw new NotSupportedException("Multi-column indices not support in table");

			return GetColumnIndex(columns[0]);
		}

		public virtual TableIndex GetColumnIndex(int column) {
			return GetColumnIndex(column, column, this);
		}

		protected virtual TableIndex GetColumnIndex(int column, int originalColumn, ITable ancestor) {
			return GetColumnIndex(column);
		}

		TableIndex IVirtualTable.GetColumnIndex(int column, int originalColumn, ITable ancestor)
			=> GetColumnIndex(column, originalColumn, ancestor);
	}
}