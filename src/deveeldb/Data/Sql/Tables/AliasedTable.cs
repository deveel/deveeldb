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

namespace Deveel.Data.Sql.Tables {
	public class AliasedTable : FilterTable, IRootTable {
		public AliasedTable(ITable table, ObjectName alias)
			: base(table) {
			TableInfo = table.TableInfo.As(alias);
		}

		public override TableInfo TableInfo { get; }

		bool IEquatable<ITable>.Equals(ITable other) {
			return this == other;
		}
	}
}