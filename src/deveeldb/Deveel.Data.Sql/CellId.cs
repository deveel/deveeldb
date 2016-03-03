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
using System.Diagnostics;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql {
	[DebuggerDisplay("ToString()")]
	public struct CellId : IEquatable<CellId> {
		public CellId(RowId rowId, int columnOffset) 
			:this() {
			RowId = rowId;
			ColumnOffset = columnOffset;
		}

		public RowId RowId { get; private set; }

		public int ColumnOffset { get; private set; }

		public bool Equals(CellId other) {
			return RowId.Equals(other.RowId) && 
				ColumnOffset.Equals(other.ColumnOffset);
		}

		public override bool Equals(object obj) {
			if (!(obj is CellId))
				return false;

			return Equals((CellId)obj);
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public override string ToString() {
			return String.Format("{0}({1})", RowId, ColumnOffset);
		}
	}
}
