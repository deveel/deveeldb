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
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Index;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	[Serializable]
	public abstract class Table : IQueryTable, ILockable {
		// Stores col name -> col index lookups
		private Dictionary<ObjectName, int> colNameLookup;
		private readonly object colLookupLock = new object();

		~Table() {
			Dispose(false);
		}

		public abstract IEnumerator<Row> GetEnumerator();

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
		}

		public abstract IDatabaseContext DatabaseContext { get; }

		public abstract TableInfo TableInfo { get; }

		public bool IsLocked { get; private set; }

		object ILockable.RefId {
			get { return TableInfo.TableName; }
		}

		int IQueryTable.ColumnCount {
			get { return ColumnCount; }
		}

		protected virtual int ColumnCount {
			get { return TableInfo.ColumnCount; }
		}

		public abstract int RowCount { get; }

		public abstract void LockRoot(int lockKey);

		public abstract void UnlockRoot(int lockKey);

		protected virtual void OnLockAcquired(Lock @lock) {
			
		}

		protected virtual void OnLockReleased(Lock @lock) {
			
		}

		void ILockable.Acquired(Lock @lock) {
			try {
				OnLockAcquired(@lock);
			} finally {
				IsLocked = true;
			}
		}

		void ILockable.Released(Lock @lock) {
			try {
				OnLockReleased(@lock);
			} finally {
				IsLocked = true;
			}
		}

		protected virtual int IndexOfColumn(ObjectName columnName) {
			return TableInfo.IndexOfColumn(columnName.Name);
		}

		protected abstract ObjectName GetResolvedColumnName(int column);

		ObjectName IQueryTable.GetResolvedColumnName(int column) {
			return GetResolvedColumnName(column);
		}

		ColumnIndex IQueryTable.GetIndex(int column, int originalColumn, ITable table) {
			return GetIndex(column, originalColumn, table);
		}

		protected abstract ColumnIndex GetIndex(int column, int originalColumn, ITable table);

		protected abstract IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor);

		IEnumerable<int> IQueryTable.ResolveRows(int columnOffset, IEnumerable<int> rows, ITable ancestor) {
			return ResolveRows(columnOffset, rows, ancestor);
		}

		protected abstract RawTableInfo GetRawTableInfo(RawTableInfo rootInfo);

		RawTableInfo IQueryTable.GetRawTableInfo(RawTableInfo rootInfo) {
			return GetRawTableInfo(rootInfo);
		}

		public abstract DataObject GetValue(long rowNumber, int columnOffset);

		public ColumnIndex GetIndex(int columnOffset) {
			return GetIndex(columnOffset, columnOffset, this);
		}

		public ObjectName FullName {
			get { return TableInfo.TableName; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Table; }
		}

		protected int FindColumn(ObjectName columnName) {
			lock (colLookupLock) {
				if (colNameLookup == null)
					colNameLookup = new Dictionary<ObjectName, int>(30);

				int index;
				if (!colNameLookup.TryGetValue(columnName, out index)) {
					index = IndexOfColumn(columnName);
					colNameLookup[columnName] = index;
				}

				return index;
			}
		}

		int IQueryTable.FindColumn(ObjectName columnName) {
			return FindColumn(columnName);
		}

		private DataType GetColumnType(int columnOffset) {
			return TableInfo[columnOffset].ColumnType;
		}

		private DataType GetColumnType(ObjectName columnName) {
			return GetColumnType(FindColumn(columnName));
		}

		ITableVariableResolver IQueryTable.GetVariableResolver() {
			return new TableVariableResolver(this);
		}

		#region TableVariableResolver

		class TableVariableResolver : ITableVariableResolver {
			public TableVariableResolver(Table table) 
				: this(table, -1) {
			}

			public TableVariableResolver(Table table, int rowIndex) {
				this.table = table;
				this.rowIndex = rowIndex;
			}

			private readonly Table table;
			private readonly int rowIndex;

			private int FindColumnName(ObjectName columnName) {
				int colIndex = table.FindColumn(columnName);
				if (colIndex == -1) {
					throw new ApplicationException("Can't find column: " + columnName);
				}
				return colIndex;
			}

			public DataObject Resolve(ObjectName columnName) {
				return table.GetValue(rowIndex, FindColumnName(columnName));
			}

			public DataType ReturnType(ObjectName columnName) {
				return table.GetColumnType(columnName);
			}

			public ITableVariableResolver ForRow(int row) {
				return new TableVariableResolver(table, row);
			}
		}

		#endregion
	}
}
