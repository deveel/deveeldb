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

using Deveel.Data.Index;

namespace Deveel.Data.Sql {
	interface IQueryTable : ITable {
		int ColumnCount { get; }

		int FindColumn(ObjectName columnName);

		IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor);

		ColumnIndex GetIndex(int column, int originalColumn, ITable table);

		ObjectName GetResolvedColumnName(int columnOffset);

		ITableVariableResolver GetVariableResolver();

		RawTableInfo GetRawTableInfo(RawTableInfo rootInfo);

		void Lock();

		void Release();
	}
}
