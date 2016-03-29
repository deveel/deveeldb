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

using Deveel.Data.Index;

namespace Deveel.Data.Sql.Tables {
	class SubsetColumnTable : FilterTable, IRootTable {
		private readonly int[] columnMap;
		private int[] reverseColumnMap;
		private TableInfo subsetTableInfo;
		private readonly ObjectName[] aliases;

		public SubsetColumnTable(ITable parent, int[] columnMap, ObjectName[] aliases) 
			: base(parent) {
			this.aliases = aliases;
			this.columnMap = columnMap;

			SetColumnMap(columnMap);
		}

		private void SetColumnMap(int[] mapping) {
			reverseColumnMap = new int[Parent.ColumnCount()];
			for (int i = 0; i < reverseColumnMap.Length; ++i) {
				reverseColumnMap[i] = -1;
			}

			var parentInfo = Parent.TableInfo;
			subsetTableInfo = new TableInfo(parentInfo.TableName);

			for (int i = 0; i < mapping.Length; ++i) {
				int mapTo = mapping[i];

				var origColumnInfo = Parent.TableInfo[mapTo];
				var columnInfo = new ColumnInfo(aliases[i].Name, origColumnInfo.ColumnType) {
					DefaultExpression = origColumnInfo.DefaultExpression,
					IsNotNull = origColumnInfo.IsNotNull,
					IndexType = origColumnInfo.IndexType
				};

				subsetTableInfo.AddColumnSafe(columnInfo);

				reverseColumnMap[mapTo] = i;
			}

			subsetTableInfo = subsetTableInfo.AsReadOnly();
		}

		protected override int ColumnCount {
			get { return aliases.Length; }
		}

		public override TableInfo TableInfo {
			get { return subsetTableInfo; }
		}

		protected override int IndexOfColumn(ObjectName columnName) {
			for (int i = 0; i < aliases.Length; ++i) {
				if (columnName.Equals(aliases[i])) {
					return i;
				}
			}
			return -1;
		}

		protected override ObjectName GetResolvedColumnName(int column) {
			return aliases[column];
		}

		protected override ColumnIndex GetIndex(int column, int originalColumn, ITable table) {
			// We need to map the original_column if the original column is a reference
			// in this subset column table.  Otherwise we leave as is.
			// The reason is because FilterTable pretends the call came from its
			// parent if a request is made on this table.
			int mappedOriginalColumn = originalColumn;
			if (table == this) {
				mappedOriginalColumn = columnMap[originalColumn];
			}

			return base.GetIndex(columnMap[column], mappedOriginalColumn, table);
		}

		protected override IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor) {
			return base.ResolveRows(columnMap[column], rowSet, ancestor);
		}

		public override Field GetValue(long rowNumber, int columnOffset) {
			return Parent.GetValue(rowNumber, columnMap[columnOffset]);
		}

		public bool TypeEquals(IRootTable other) {
			return this == other;
		}
	}
}
