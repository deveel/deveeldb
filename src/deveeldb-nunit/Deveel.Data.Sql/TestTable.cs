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
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Index;

namespace Deveel.Data.Sql {
	public class TestTable : ITable {
		private readonly List<Row> rows;

		public TestTable(TableInfo tableInfo, IEnumerable<DataObject[]> rows) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");

			TableInfo = tableInfo;
			this.rows = new List<Row>();

			if (rows != null) {
				int rowNum = -1;
				foreach (var row in rows) {
					var r = new Row(this, ++rowNum);
					for (int i = 0; i < row.Length; i++) {
						r.SetValue(i, row[i]);
					}

					this.rows.Add(r);
				}
			}
		}

		public virtual ObjectName FullName {
			get { return TableInfo.TableName; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Table; }
		}

		public virtual IEnumerator<Row> GetEnumerator() {
			return rows.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public virtual void Dispose() {
		}

		public virtual TableInfo TableInfo { get; private set; }

		public virtual int RowCount {
			get { return rows.Count; }
		}

		public virtual DataObject GetValue(long rowNumber, int columnOffset) {
			var row = rows.FirstOrDefault(x => x.RowId.RowNumber == rowNumber);
			if (row == null)
				throw new ArgumentOutOfRangeException("rowNumber");

			return row.GetValue(columnOffset);
		}

		public virtual TableIndex GetIndex(int columnOffset) {
			return new BlindSearchIndex(this, columnOffset);
		}
	}
}
