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

using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class TableInfo : IEnumerable<ColumnInfo> {
		private readonly List<ColumnInfo> columns;
		private readonly Dictionary<string, int> columnsCache;
 
		public TableInfo(ObjectName tableName) {
			TableName = tableName;
			columns = new List<ColumnInfo>();
			columnsCache = new Dictionary<string, int>();
			IgnoreCase = true;
		}

		public ObjectName TableName { get; private set; }

		public string Name {
			get { return TableName.Name; }
		}

		public ObjectName SchemaName {
			get { return TableName.Parent; }
		}

		public string CatalogName {
			get { return SchemaName != null && SchemaName.Parent != null ? SchemaName.Parent.Name : null; }
		}

		public bool IgnoreCase { get; set; }

		public bool IsReadOnly { get; private set; }

		public int ColumnCount {
			get { return columns.Count; }
		}

		public ColumnInfo this[int offset] {
			get { return columns[offset]; }
		}

		private void AssertNotReadOnly() {
			if (IsReadOnly)
				throw new InvalidOperationException();
		}

		public void AddColumn(ColumnInfo column) {
			if (column == null)
				throw new ArgumentNullException("column");

			AssertNotReadOnly();

			if (column.TableInfo != null &&
			    column.TableInfo != this)
				throw new ArgumentException();

			if (columns.Any(x => x.ColumnName == column.ColumnName))
				throw new ArgumentException(String.Format("Column {0} is already defined in table {1}", column.ColumnName, TableName));

			column.TableInfo = this;
			columns.Add(column);
		}

		public ColumnInfo AddColumn(string columnName, DataType columnType) {
			return AddColumn(columnName, columnType, false);
		}

		public ColumnInfo AddColumn(string columnName, DataType columnType, bool notNull) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			if (columnType == null)
				throw new ArgumentNullException("columnType");

			var column = new ColumnInfo(columnName, columnType);
			column.IsNotNull = notNull;
			AddColumn(column);
			return column;
		}

		public IEnumerator<ColumnInfo> GetEnumerator() {
			return columns.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public int IndexOfColumn(string columnName) {
			int index;
			if (!columnsCache.TryGetValue(columnName, out index)) {
				for (int i = 0; i < columns.Count; i++) {
					var column = columns[i];
					if (column.ColumnName == columnName) {
						index = i;
						columnsCache[columnName] = index;
					}
				}
			}

			return index;
		}
	}
}