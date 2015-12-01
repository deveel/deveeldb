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
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Caching {
	public sealed class CellKey : IEquatable<CellKey> {
		private readonly string database;
		private readonly CellId cellId;

		internal CellKey(string database, CellId cellId) {
			if (String.IsNullOrEmpty(database))
				throw new ArgumentNullException("database");

			if (cellId.RowId.IsNull)
				throw new ArgumentException("Null ROWID in key.","cellId");
			if (cellId.ColumnOffset < 0)
				throw new ArgumentOutOfRangeException("cellId", "The column offset in the key is smaller than zero.");

			this.database = database;
			this.cellId = cellId;
		}

		public string Database {
			get { return database; }
		}

		public RowId RowId {
			get { return cellId.RowId; }
		}

		public int ColumnOffset {
			get { return cellId.ColumnOffset; }
		}

		public bool Equals(CellKey other) {
			if (other == null)
				return false;

			return String.Equals(database, other.database) &&
			       cellId.Equals(other.cellId);
		}

		public override bool Equals(object obj) {
			var other = obj as CellKey;
			return Equals(other);
		}

		public override int GetHashCode() {
			unchecked {
				return ((database != null ? database.GetHashCode() : 0)*397) ^ cellId.GetHashCode();
			}
		}
	}
}
