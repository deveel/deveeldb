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
using System.Linq;

using Deveel.Data;
using Deveel.Data.Index;

namespace Deveel.Data.Sql.Tables {
	public abstract class BaseDataTable : RootTable {
		private readonly IDatabaseContext context;

		private ColumnIndex[] indexes;

		protected BaseDataTable() 
			: this(null) {
		}

		protected BaseDataTable(IDatabaseContext context) {
			this.context = context;
		}

		public override IDatabaseContext DatabaseContext {
			get { return context; }
		}

		protected override ObjectName GetResolvedColumnName(int column) {
			var columnName = TableInfo[column].ColumnName;
			return new ObjectName(TableName, columnName);
		}

		protected override int IndexOfColumn(ObjectName columnName) {
			// Check this is the correct table first...
			var tableName = columnName.Parent;
			var tableInfo = TableInfo;
			if (tableName != null && tableName.Equals(TableName)) {
				// Look for the column name
				string colName = columnName.Name;
				int size = ColumnCount;
				for (int i = 0; i < size; ++i) {
					var col = tableInfo[i];
					if (col.ColumnName.Equals(colName)) {
						return i;
					}
				}
			}

			return -1;
		}

		protected virtual ColumnIndex GetColumnIndex(int columnOffset) {
			return indexes[columnOffset];
		}

		protected override ColumnIndex GetIndex(int column, int originalColumn, ITable table) {
			var index = GetColumnIndex(column);
			if (table == this)
				return index;

			// Otherwise, get the scheme to calculate a subset of the given scheme.
			return index.GetSubset(table, originalColumn);
		}

		protected void SetupIndexes(string indexTypeName) {
			Type indexType;
			if (String.Equals(indexTypeName, DefaultIndexTypes.BlindSearch)) {
				indexType = typeof (BlindSearchIndex);
			} else if (String.Equals(indexTypeName, DefaultIndexTypes.InsertSearch)) {
				indexType = typeof (InsertSearchIndex);
			} else {
#if PCL
				indexType = Type.GetType(indexTypeName, false);
#else
				indexType = Type.GetType(indexTypeName, false, true);
#endif
			}

			if (indexType == null) {
				indexType = typeof (BlindSearchIndex);
			} else if (!typeof (ColumnIndex).IsAssignableFrom(indexType)) {
				throw new InvalidOperationException(String.Format("The type '{0}' is not a valid table index.", indexType));
			}

			SetupIndexes(indexType);
		}

		protected virtual void SetupIndexes(Type indexType) {
			indexes = new ColumnIndex[ColumnCount];
			for (int i = 0; i < ColumnCount; i++) {
				if (indexType == typeof (BlindSearchIndex)) {
					indexes[i] = new BlindSearchIndex(this, i);
				} else if (indexType == typeof (InsertSearchIndex)) {
					indexes[i] = new InsertSearchIndex(this, i);
				} else {
					var index = Activator.CreateInstance(indexType, this, i) as ColumnIndex;
					if (index == null)
						throw new InvalidOperationException();

					indexes[i] = index;
				}
			}			
		}

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			var rows = this.Select(row => row.RowId.RowNumber).ToList();
			rootInfo.Add(this, rows);
			return rootInfo;
		}

		protected override IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor) {
			if (ancestor != this)
				throw new Exception("Method routed to incorrect table ancestor.");

			return rowSet;
		}

		public void AddToIndex(int rowNumber, int columnNumber) {
			bool indexableType = TableInfo[columnNumber].IsIndexable;
			if (indexableType) {
				var index = GetColumnIndex(columnNumber);
				index.Insert(rowNumber);
			}
		}

		public void AddRowToIndex(int rowNumber) {
			int colCount = ColumnCount;
			var tableInfo = TableInfo;
			for (int i = 0; i < colCount; ++i) {
				if (tableInfo[i].IsIndexable) {
					var index = GetColumnIndex(i);
					index.Insert(rowNumber);
				}
			}
		}

		public void RemoveRowFromIndex(int rowNumber) {
			int colCount = ColumnCount;
			var tableInfo = TableInfo;
			for (int i = 0; i < colCount; ++i) {
				if (tableInfo[i].IsIndexable) {
					var index = GetColumnIndex(i);
					index.Remove(rowNumber);
				}
			}
		}
	}
}
