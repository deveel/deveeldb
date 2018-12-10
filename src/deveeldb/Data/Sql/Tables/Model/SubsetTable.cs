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
using System.Threading.Tasks;

namespace Deveel.Data.Sql.Tables.Model {
	public class SubsetTable : FilterTable, IRootTable {
		private readonly int[] columns;
		private readonly ObjectName[] aliases;
		private readonly TableInfo tableInfo;

		public SubsetTable(ITable table, int[] columns, ObjectName[] aliases)
			: base(table) {
			if (columns.Length != aliases.Length)
				throw new ArgumentException("The number of column offsets and the number of aliases do not match");

			this.columns = columns;
			this.aliases = aliases;

			var parentInfo = Parent.TableInfo;
			tableInfo = new TableInfo(parentInfo.TableName);

			for (int i = 0; i < columns.Length; ++i) {
				int mapTo = columns[i];

				var origColumnInfo = Parent.TableInfo.Columns[mapTo];
				var columnInfo = new ColumnInfo(aliases[i].Name, origColumnInfo.ColumnType) {
					DefaultValue = origColumnInfo.DefaultValue
				};

				tableInfo.Columns.Add(columnInfo);
			}

			tableInfo = tableInfo.AsReadOnly();
		}

		public override TableInfo TableInfo => tableInfo;

		protected override IEnumerable<long> ResolveRows(int column, IEnumerable<long> rows, ITable ancestor) {
			return base.ResolveRows(columns[column], rows, ancestor);
		}

		public override Task<SqlObject> GetValueAsync(long row, int column) {
			return base.GetValueAsync(row, columns[column]);
		}

		bool IEquatable<ITable>.Equals(ITable other) {
			return this == other;
		}
	}
}