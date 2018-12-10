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
using System.Threading.Tasks;

using Deveel.Data.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	public class RowReferenceResolver : IReferenceResolver {
		private readonly ITable table;
		private readonly long row;

		public RowReferenceResolver(ITable table, long row) {
			this.table = table;
			this.row = row;
		}

		public Task<SqlObject> ResolveReferenceAsync(ObjectName referenceName) {
			var columnIndex = table.TableInfo.Columns.IndexOf(referenceName);
			if (columnIndex < 0)
				return null;

			return table.GetValueAsync(row, columnIndex);
		}

		public SqlType ResolveType(ObjectName referenceName) {
			var columnIndex = table.TableInfo.Columns.IndexOf(referenceName);
			if (columnIndex < 0)
				return null;

			return table.TableInfo.Columns[columnIndex].ColumnType;
		}
	}
}