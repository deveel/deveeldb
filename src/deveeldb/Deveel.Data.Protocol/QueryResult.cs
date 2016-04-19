// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Protocol {
	public sealed class QueryResult : IDisposable {
		private QueryResultColumn[] columns;
		private TemporaryTable localTable;

		internal QueryResult(SqlQuery query, StatementResult result, bool readAll) {
			Query = query;
			Result = result;

			FormColumns(Result);

			if (readAll && Result.Type == StatementResultType.CursorRef)
				ReadAll();
		}

		~QueryResult() {
			Dispose(false);
		}

		private void ReadAll() {
			if (Result.Type == StatementResultType.CursorRef) {
				var tableInfo = Result.Cursor.Source.TableInfo;
				localTable = new TemporaryTable("##LOCAL##", tableInfo);

				foreach (var row in Result.Cursor) {
					var rowIndex = localTable.NewRow();
					for (int i = 0; i < row.ColumnCount; i++) {
						localTable.SetValue(rowIndex, i, row.GetValue(i));
					}
				}
			}
		}

		private void FormColumns(StatementResult result) {
			if (result.Type == StatementResultType.Exception)
				return;

			IEnumerator<Row> enumerator = null;
			if (result.Type == StatementResultType.CursorRef) {
				enumerator = result.Cursor.GetEnumerator();
			} else if (result.Type == StatementResultType.Result) {
				enumerator = result.Result.GetEnumerator();
			}

			try {
				if (enumerator != null) {
					if (enumerator.MoveNext()) {
						var row = enumerator.Current;

						if (row != null) {
							for (int c = 0; c < row.ColumnCount; ++c) {
								row.GetValue(c);
							}
						}
					}
				}
			} finally {
				if (enumerator != null)
					enumerator.Dispose();
			}


			TableInfo tableInfo;
			if (result.Type == StatementResultType.CursorRef) {
				tableInfo = result.Cursor.Source.TableInfo;
			} else {
				tableInfo = result.Result.TableInfo;
			}

			var columnCount = tableInfo.ColumnCount;

			ColumnCount = columnCount;

			columns = new QueryResultColumn[columnCount];

			ITable source = null;
			if (result.Type == StatementResultType.Result) {
				source = result.Result;
			} else if (result.Type == StatementResultType.CursorRef) {
				source = result.Cursor.Source;
			} else {
				return;
			}

			for (int i = 0; i < columnCount; ++i) {
				var v = source.GetResolvedColumnName(i);
				string fieldName;
				if (v.ParentName == null) {
					// This means the column is an alias
					fieldName = String.Format("@a{0}", v.Name);
				} else {
					// This means the column is an schema/table/column reference
					fieldName = String.Format("@f{0}", v);
				}

				columns[i] = new QueryResultColumn(fieldName, tableInfo[i]);
			}

			RowCount = source.RowCount;
		}

		public SqlQuery Query { get; private set; }

		public StatementResult Result { get; private set; }

		public int RowCount { get; private set; }

		public int ColumnCount { get; private set; }

		public QueryResultColumn GetColumn(int columnOffset) {
			if (columnOffset < 0 || columnOffset >= ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			return columns[columnOffset];
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (localTable != null)
					localTable.Dispose();
				if (Result != null)
					Result.Dispose();
			}

			Result = null;
			Query = null;
			columns = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public Field GetValue(int rowIndex, int columnIndex) {
			// TODO: Ensure to fetch the next row
			if (localTable != null) {
				return localTable.GetValue(rowIndex, columnIndex);
			} else if (Result.Type == StatementResultType.CursorRef) {
				var row = Result.Cursor.Fetch(FetchDirection.Absolute, rowIndex);
				if (row == null)
					return Field.Null();

				return row.GetValue(columnIndex);
			}

			var obj = Result.Result.GetValue(rowIndex, columnIndex);

			// TODO: support large object references

			return obj;
		}
	}
}
