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

using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Triggers {
	class OldAndNewTableContainer : ITableContainer {
		private readonly ITransaction transaction;

		public OldAndNewTableContainer(ITransaction transaction) {
			this.transaction = transaction;
		}

		private ITableStateHandler Handler {
			get {
				AssertTableStateHandler();
				return (ITableStateHandler) transaction;
			}
		}

		private bool HasOldTable {
			get { return Handler.TableState.OldRowIndex != -1; }
		}

		private bool HasNewTable {
			get { return Handler.TableState.NewDataRow != null; }
		}


		public int TableCount {
			get {
				int count = 0;
				if (HasOldTable)
					++count;
				if (HasNewTable)
					++count;
				return count;
			}
		}

		private void AssertTableStateHandler() {
			if (!(transaction is ITableStateHandler))
				throw new InvalidOperationException("The transaction does not handle trigger states.");
		}

		public int FindByName(ObjectName name) {
			if (HasOldTable &&
				name.Equals(SystemSchema.OldTriggerTableName))
				return 0;

			if (HasNewTable &&
				name.Equals(SystemSchema.NewTriggerTableName))
				return HasOldTable ? 1 : 0;

			return -1;
		}

		public ObjectName GetTableName(int offset) {
			if (HasOldTable && offset == 0)
				return SystemSchema.OldTriggerTableName;

			return SystemSchema.NewTriggerTableName;
		}

		public TableInfo GetTableInfo(int offset) {
			var tableInfo = transaction.GetTableInfo(Handler.TableState.TableSource);
			return tableInfo.Alias(GetTableName(offset));
		}

		public string GetTableType(int offset) {
			return TableTypes.SystemTable;
		}

		public bool ContainsTable(ObjectName name) {
			return FindByName(name) > 0;
		}

		public ITable GetTable(int offset) {
			var tableInfo = GetTableInfo(offset);

			var table = new TriggeredOldNew(transaction.Context, tableInfo);

			if (HasOldTable) {
				if (offset == 0) {
					// Copy data from the table to the new table
					var dtable = transaction.GetTable(Handler.TableState.TableSource);
					var oldRow = new Row(table);
					int rowIndex = Handler.TableState.OldRowIndex;
					for (int i = 0; i < tableInfo.ColumnCount; ++i) {
						oldRow.SetValue(i, dtable.GetValue(rowIndex, i));
					}

					// All OLD tables are immutable
					table.SetReadOnly(true);
					table.SetData(oldRow);

					return table;
				}
			}

			table.SetReadOnly(!Handler.TableState.IsNewMutable);
			table.SetData(Handler.TableState.NewDataRow);

			return table;
		}

		#region TriggeredOldNew

		class TriggeredOldNew : GeneratedTable, IMutableTable {
			private readonly TableInfo tableInfo;
			private Row data;
			private bool readOnly;

			public TriggeredOldNew(IContext context, TableInfo tableInfo)
				: base(context) {
				this.tableInfo = tableInfo;
			}

			public override TableInfo TableInfo {
				get { return tableInfo; }
			}

			public override int RowCount {
				get { return 1; }
			}

			public void SetData(Row row) {
				data = row;
			}

			public void SetReadOnly(bool flag) {
				readOnly = flag;
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber >= 1)
					throw new ArgumentOutOfRangeException("rowNumber");

				return data.GetValue(columnOffset);
			}

			public TableEventRegistry EventRegistry {
				get { throw new InvalidOperationException(); }
			}

			public RowId AddRow(Row row) {
				throw new NotSupportedException(String.Format("Inserting data into '{0}' is not allowed.", tableInfo.TableName));
			}

			public void UpdateRow(Row row) {
				if (row.RowId.RowNumber < 0 ||
					row.RowId.RowNumber >= 1)
					throw new ArgumentOutOfRangeException();
				if (readOnly)
					throw new NotSupportedException(String.Format("Updating '{0}' is not permitted.", tableInfo.TableName));

				int sz = TableInfo.ColumnCount;
				for (int i = 0; i < sz; ++i) {
					data.SetValue(i, row.GetValue(i));
				}
			}

			public bool RemoveRow(RowId rowId) {
				throw new NotSupportedException(String.Format("Deleting data from '{0}' is not allowed.", tableInfo.TableName));
			}

			public void FlushIndexes() {
			}

			public void AssertConstraints() {
			}

			public void AddLock() {
			}

			public void RemoveLock() {
			}
		}

		#endregion
	}
}
