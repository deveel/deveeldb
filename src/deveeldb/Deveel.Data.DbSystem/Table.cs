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

using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	[Serializable]
	public abstract class Table : IDbTable {
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

		protected abstract IDatabase Database { get; }

		IDatabase IDbTable.Database {
			get { return Database; }
		}

		public abstract TableInfo TableInfo { get; }

		public abstract int ColumnCount { get; }

		public abstract int RowCount { get; }

		public abstract bool HasRootsLocked { get; }

		public abstract void LockRoot(int lockKey);

		public abstract void UnlockRoot(int lockKey);

		protected abstract int IndexOfColumn(ObjectName columnName);

		protected abstract ObjectName GetResolvedColumnName(int column);

		ObjectName IDbTable.GetResolvedColumnName(int column) {
			return GetResolvedColumnName(column);
		}

		ColumnIndex IDbTable.GetIndex(int column, int originalColumn, ITable table) {
			return GetIndex(column, originalColumn, table);
		}

		protected abstract ColumnIndex GetIndex(int column, int originalColumn, ITable table);

		protected abstract IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor);

		IEnumerable<int> IDbTable.ResolveRows(int columnOffset, IEnumerable<int> rows, ITable ancestor) {
			return ResolveRows(columnOffset, rows, ancestor);
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

		public int FindColumn(ObjectName columnName) {
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

		public DataType GetColumnType(int columnOffset) {
			return TableInfo[columnOffset].ColumnType;
		}

		public DataType GetColumnType(ObjectName columnName) {
			return GetColumnType(FindColumn(columnName));
		}

		ITableVariableResolver IDbTable.GetVariableResolver() {
			return new TableVariableResolver(this);
		}

		#region TableVariableResolver

		class TableVariableResolver : ITableVariableResolver {
			public TableVariableResolver(Table table) {
				this.table = table;
			}

			private readonly Table table;
			private int rowIndex = -1;

			private int FindColumnName(ObjectName columnName) {
				int colIndex = table.FindColumn(columnName);
				if (colIndex == -1) {
					throw new ApplicationException("Can't find column: " + columnName);
				}
				return colIndex;
			}

			public int SetId {
				get { return rowIndex; }
			}

			public void AssignSetId(int value) {
				rowIndex = value;
			}

			public DataObject Resolve(ObjectName columnName) {
				return table.GetValue(FindColumnName(columnName), rowIndex);
			}

			public DataType ReturnType(ObjectName columnName) {
				return table.GetColumnType(columnName);
			}

		}

		#endregion
	}
}
