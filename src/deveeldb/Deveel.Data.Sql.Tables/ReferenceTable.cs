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

namespace Deveel.Data.Sql.Tables {
	class ReferenceTable : FilterTable, IRootTable {
		private readonly TableInfo tableInfo;

		public ReferenceTable(ITable parent, ObjectName tableName) 
			: this(parent, parent.TableInfo.Alias(tableName)) {
		}

		public ReferenceTable(ITable parent, TableInfo tableInfo)
			: base(parent) {
			TableName = tableInfo.TableName;
			this.tableInfo = tableInfo;
		}

		public ObjectName TableName { get; private set; }

		public override TableInfo TableInfo {
			get { return tableInfo; }
		}

		protected override int IndexOfColumn(ObjectName columnName) {
			var tableName = columnName.Parent;
			if (tableName != null && tableName.Equals(TableName))
				return TableInfo.IndexOfColumn(columnName.Name);

			return -1;
		}

		protected override ObjectName GetResolvedColumnName(int column) {
			return new ObjectName(TableName, tableInfo[column].ColumnName);
		}

		public bool TypeEquals(IRootTable other) {
			return other == this;
		}
	}
}
