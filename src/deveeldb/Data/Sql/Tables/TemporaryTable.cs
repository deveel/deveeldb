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
using System.Threading.Tasks;

using Deveel.Data.Sql.Indexes;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	public class TemporaryTable : DataTableBase {
		private List<SqlObject[]> rows;

		public TemporaryTable(TableInfo tableInfo) {
			TableInfo = tableInfo.AsReadOnly();
			rows = new List<SqlObject[]>();
		}

		public TemporaryTable(TableInfo tableInfo, ObjectName alias)
			: this(tableInfo.As(alias)) {
		}

		public TemporaryTable(ObjectName tableName, IEnumerable<ColumnInfo> columns)
			: this(MakeTableInfo(tableName, columns)) {
		}

		static TemporaryTable() {
			var table = new TemporaryTable(new ObjectName("SINGLE_ROW_TABLE"), new ColumnInfo[0]);
			table.NewRow();
			SingleRow = table;
		}

		public static ITable SingleRow { get; }

		public override TableInfo TableInfo { get; }

		public override long RowCount => rows.Count;

		private static TableInfo MakeTableInfo(ObjectName tableName, IEnumerable<ColumnInfo> columns) {
			var tableInfo = new TableInfo(tableName);

			foreach (var column in columns) {
				tableInfo.Columns.Add(column);
			}

			return tableInfo;
		}

		public override IEnumerator<Row> GetEnumerator() {
			return new SimpleRowEnumerator(this);
		}

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			var tableRows = rows.Select((item, index) => (long) index).ToBigList();
			rootInfo.Add(this, tableRows);

			return rootInfo;
		}

		public override Task<SqlObject> GetValueAsync(long row, int column) {
			if (row > Int32.MaxValue)
				throw new ArgumentOutOfRangeException("row");

			if (row < 0 || row >= rows.Count)
				throw new ArgumentOutOfRangeException(nameof(row));

			var values = rows[(int) row];

			return Task.FromResult(values[column]);
		}

		public void SetValue(long row, int column, SqlObject value) {
			if (row < 0 || row >= rows.Count)
				throw new ArgumentOutOfRangeException(nameof(row));

			var values = rows[(int) row];
			values[column] = value;
		}

		public void AddRow(SqlObject[] values) {
			if (values.Length != TableInfo.Columns.Count)
				throw new ArgumentException();

			rows.Add(values);
		}

		public int NewRow() {
			rows.Add(new SqlObject[TableInfo.Columns.Count]);

			return rows.Count - 1;
		}

		public static TemporaryTable SingleColumnTable(string columnName, SqlType columnType) {
			var tableInfo = new TableInfo(new ObjectName("single"));
			tableInfo.Columns.Add(new ColumnInfo(columnName, columnType));

			return new TemporaryTable(tableInfo);
		}

		public void BuildIndex() {
			SetupIndexes(typeof(BlindSearchIndex));

			for (int i = 0; i < RowCount; i++) {
				AddRowToIndex(i);
			}
		}
	}
}