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

using Deveel.Data.Sql;

namespace Deveel.Data.Caching {
	public sealed class CachedCell {
		internal CachedCell(RowId rowId, int columnOffset, DataObject value) {
			if (rowId.IsNull)
				throw new ArgumentNullException("rowId");
			if (columnOffset < 0)
				throw new ArgumentOutOfRangeException("columnOffset");

			RowId = rowId;
			ColumnOffset = columnOffset;
			Value = value;
		}

		public RowId RowId { get; private set; }

		public int TableId {
			get { return RowId.TableId; }
		}

		public long RowNumber {
			get { return RowId.RowNumber; }
		}

		public int ColumnOffset { get; private set; }

		public DataObject Value { get; private set; }
	}
}
