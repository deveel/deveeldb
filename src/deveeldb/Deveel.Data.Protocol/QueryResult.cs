// 
//  Copyright 2010-2015 Deveel
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
using System.Resources;

using Deveel.Data.Sql;

namespace Deveel.Data.Protocol {
	public sealed class QueryResult : IDisposable {
		private QueryResultColumn[] columns;

		private bool resultIsSimpleEnum;
		private IList<int> rowIndexMap;

		private int locked;

		internal QueryResult(SqlQuery query, ITable result) {
			Query = query;
			Result = result;
			FormColumns(Result);

			locked = 0;
		}

		private void FormColumns(ITable result) {
			// HACK: Read the contents of the first row so that we can pick up
			//   any errors with reading, and also to fix the 'uniquekey' bug
			//   that causes a new transaction to be started if 'uniquekey' is
			//   a column and the value is resolved later.
			var columnCount = result.TableInfo.ColumnCount;
			using (var rowEnum = result.GetEnumerator()) {
				if (rowEnum.MoveNext()) {
					int rowIndex = rowEnum.Current.RowId.RowNumber;
					for (int c = 0; c < columnCount; ++c) {
						result.GetValue(rowIndex, c);
					}
				}

				// If simple enum, note it here
				resultIsSimpleEnum = (rowEnum is SimpleRowEnumerator);
			}

			// Build 'row_index_map' if not a simple enum
			if (!resultIsSimpleEnum) {
				rowIndexMap = new List<int>(result.RowCount);

				var en = result.GetEnumerator();
				while (en.MoveNext()) {
					rowIndexMap.Add(en.Current.RowId.RowNumber);
				}
			}

			// This is a safe operation provides we are shared.
			// Copy all the TableField columns from the table to our own
			// QueryResultColumn array, naming each column by what is returned from
			// the 'GetResolvedVariable' method.
			int colCount = result.TableInfo.ColumnCount;
			columns = new QueryResultColumn[colCount];
			for (int i = 0; i < colCount; ++i) {
				var v = result.GetResolvedColumnName(i);
				string fieldName;
				if (v.ParentName == null) {
					// This means the column is an alias
					fieldName = String.Format("@a{0}", v.Name);
				} else {
					// This means the column is an schema/table/column reference
					fieldName = String.Format("@f{0}", v);
				}

				columns[i] = new QueryResultColumn(fieldName, result.TableInfo[i]);
			}
		}

		public SqlQuery Query { get; private set; }

		public ITable Result { get; private set; }

		public int RowCount {
			get { return Result.RowCount; }
		}

		public int ColumnCount {
			get { return Result.TableInfo.ColumnCount; }
		}

		public QueryResultColumn GetColumn(int columnOffset) {
			if (columnOffset < 0 || columnOffset >= ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			return columns[columnOffset];
		}

		public void LockRoot(int lockKey) {
			Result.LockRoot(lockKey);
			locked++;
		}

		public void UnlockRoot(int lockKey) {
			Result.UnlockRoot(lockKey);
			--locked;
		}

		public void Dispose() {
			if (locked > 0) {
				UnlockRoot(-1);
			}

			Result = null;
			Query = null;
			columns = null;
			rowIndexMap = null;
		}

		public DataObject GetValue(int rowIndex, int columnIndex) {
			if (locked <= 0)
				throw new Exception("Table roots not locked!");

			int realRow = resultIsSimpleEnum ? rowIndex : rowIndexMap[rowIndex];
			var obj = Result.GetValue(realRow, columnIndex);

			// TODO: support large object references

			return obj;
		}
	}
}
