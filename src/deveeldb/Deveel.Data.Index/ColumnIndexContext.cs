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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Index {
	public sealed class ColumnIndexContext {
		internal ColumnIndexContext(ITable table, int columnOffset, IEnumerable<KeyValuePair<string, object>> metadata) {
			Table = table;
			ColumnOffset = columnOffset;
			Metadata = metadata;
		}

		public ITable Table { get; private set; }

		public string ColumnName {
			get { return Table.TableInfo[ColumnOffset].ColumnName; }
		}

		public SqlType ColumnType {
			get { return Table.TableInfo[ColumnOffset].ColumnType; }
		}

		public int ColumnOffset { get; private set; }

		public IEnumerable<KeyValuePair<string, object>> Metadata { get; private set; }
	}
}
