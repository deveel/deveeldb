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

using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	public class TemporaryTable : BaseDataTable {
		private readonly TableInfo tableInfo;
		private int rowCount;
		private List<DataObject[]> rows;

		public TemporaryTable(TableInfo tableInfo)
			: this((IDatabase)null, tableInfo) {
		}

		public TemporaryTable(IDatabase database, TableInfo tableInfo)
			: base(database) {
			this.tableInfo = tableInfo.AsReadOnly();
			rows = new List<DataObject[]>();
		}

		public TemporaryTable(string name, TableInfo sourceTableInfo)
			: this(null, name, sourceTableInfo) {
		}

		public TemporaryTable(IDatabase database, string name, TableInfo sourceTableInfo)
			: this(database, sourceTableInfo.Alias(new ObjectName(name))) {
		}

		public override TableInfo TableInfo {
			get { return tableInfo; }
		}

		public override int RowCount {
			get { return rowCount; }
		}

		public override bool HasRootsLocked {
			get { return true; }
		}

		public int NewRow() {
			rows.Add(new DataObject[ColumnCount]);
			++rowCount;
			return rowCount - 1;
		}

		public int NewRow(DataObject[] row) {
			if (row == null)
				throw new ArgumentNullException("row");
			if (row.Length != ColumnCount)
				throw new ArgumentException();

			var rowNumber = NewRow();
			for (int i = 0; i < row.Length; i++) {
				SetValue(rowNumber, i, row[i]);
			}

			return rowNumber;
		}

		public override DataObject GetValue(long rowNumber, int columnOffset) {
			if (rowNumber >= rows.Count)
				throw new ArgumentOutOfRangeException("rowNumber");
			if (columnOffset < 0 || columnOffset > ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			var row = rows[(int) rowNumber];
			return row[columnOffset];
		}

		public void SetValue(long rowNumber, int columnOffset, DataObject value) {
			if (rowNumber < 0 || rowNumber >= rows.Count)
				throw new ArgumentOutOfRangeException("rowNumber");
			if (columnOffset < 0 || columnOffset >= ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			var row = rows[(int) rowNumber];
			row[columnOffset] = value;
		}

		public void SetValue(int columnOffset, DataObject value) {
			SetValue(rowCount - 1, columnOffset, value);
		}

		public void BuildIndexes() {
			BuildIndexes(DefaultIndexNames.InsertSearch);
		}

		public void BuildIndexes(string indexName) {
			SetupIndexes(indexName);

			for (int i = 0; i < rowCount; i++) {
				AddRowToIndex(i);
			}			
		}

		public void CopyFrom(ITable table, int row) {
			if (!(table is IDbTable))
				throw new ArgumentException();

			var dbTable = (IDbTable) table;

			NewRow();

			var columnNames = new ObjectName[dbTable.ColumnCount];
			for (int i = 0; i < columnNames.Length; ++i) {
				columnNames[i] = dbTable.GetResolvedColumnName(i);
			}

			for (int i = 0; i < ColumnCount; ++i) {
				var v = GetResolvedColumnName(i);
				var colName = v.Name;

				try {
					int columnOffset = -1;
					for (int n = 0; n < columnNames.Length || columnOffset == -1; ++n) {
						if (columnNames[n].Name.Equals(colName)) {
							columnOffset = n;
						}
					}

					var value = table.GetValue(row, columnOffset);
					SetValue(rowCount-1, i, value);
				} catch (Exception e) {
					throw new ApplicationException(e.Message, e);
				}
			}
		}


		public override IEnumerator<Row> GetEnumerator() {
			return new SimpleRowEnumerator(this);
		}

		public override void LockRoot(int lockKey) {
		}

		public override void UnlockRoot(int lockKey) {
		}

		public static TemporaryTable SingleColumnTable(IDatabase database, string columnName, DataType columnType) {
			var tableInfo = new TableInfo(new ObjectName("single"));
			tableInfo.AddColumn(columnName, columnType);
			tableInfo = tableInfo.AsReadOnly();
			return new TemporaryTable(database, tableInfo);
		}
	}
}
