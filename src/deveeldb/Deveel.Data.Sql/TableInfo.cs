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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class TableInfo {
		public TableInfo(ObjectName tableName) {
			TableName = tableName;
			Columns = new ColumnCollection(this);
			IgnoreCase = true;
		}

		public ObjectName TableName { get; private set; }

		public ICollection<ColumnInfo> Columns { get; private set; }

		public bool IgnoreCase { get; set; }

		#region ColumnCollection

		class ColumnCollection : Collection<ColumnInfo> {
			private readonly TableInfo tableInfo;

			public ColumnCollection(TableInfo tableInfo) {
				this.tableInfo = tableInfo;
			}

			protected override void InsertItem(int index, ColumnInfo item) {
				if (item == null)
					throw new ArgumentNullException("item");

				if (item.TableInfo != null)
					throw new ArgumentException(String.Format("The column {0} already belongs to the table {1}: cannot double add.",
						item.ColumnName,
						item.TableInfo.TableName));
				if (Items.Any(x => String.Equals(x.ColumnName, item.ColumnName, tableInfo.IgnoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)))
					throw new ArgumentException(String.Format("Table {0} already defined a column named {1}", tableInfo.TableName, item.ColumnName));

				item.TableInfo = tableInfo;
				base.InsertItem(index, item);
			}
		}

		#endregion
	}
}