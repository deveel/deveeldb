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
using System.Linq;

namespace Deveel.Data.Sql.Tables {
	public static class TableExtensions {
		internal static RawTableInfo GetRawTableInfo(this ITable table, RawTableInfo rootInfo) {
			if (table is IVirtualTable)
				return ((IVirtualTable) table).GetRawTableInfo(rootInfo);
			if (table is IRootTable)
				return ((IRootTable) table).GetRawTableInfo(rootInfo);

			throw new NotSupportedException();
		}

		internal static RawTableInfo GetRawTableInfo(this ITable table)
			=> table.GetRawTableInfo(new RawTableInfo());

		internal static RawTableInfo GetRawTableInfo(this IRootTable table, RawTableInfo rootInfo) {
			var rows = table.Select(x => x.Number).ToBigList();
			rootInfo.Add(table, rows);

			return rootInfo;
		}

		internal static IEnumerable<long> ResolveRows(this ITable table, int column, IEnumerable<long> rows,
			ITable ancestor) {
			if (table is IVirtualTable)
				return ((IVirtualTable) table).ResolveRows(column, rows, ancestor);

			if (table != ancestor)
				throw new InvalidOperationException();

			return rows;
		}

	}
}