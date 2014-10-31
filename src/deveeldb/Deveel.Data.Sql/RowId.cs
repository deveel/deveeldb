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

namespace Deveel.Data.Sql {
	public struct RowId : IEquatable<RowId> {
		public RowId(int tableId, long rowNumber) 
			: this(false) {
			RowNumber = rowNumber;
			TableId = tableId;
		}

		private RowId(bool isNull)
			: this() {
			IsNull = isNull;
		}

		public int TableId { get; private set; }

		public long RowNumber { get; private set; }

		public bool IsNull { get; private set; }

		public bool Equals(RowId other) {
			if (IsNull && other.IsNull)
				return true;

			return TableId.Equals(other.TableId) &&
			       RowNumber.Equals(other.RowNumber);
		}

		public override bool Equals(object obj) {
			if (!(obj is RowId))
				return false;

			return Equals((RowId) obj);
		}

		public override int GetHashCode() {
			if (IsNull)
				return 0;

			return unchecked (TableId.GetHashCode() ^ RowNumber.GetHashCode());
		}

		public override string ToString() {
			return base.ToString();
		}
	}
}
