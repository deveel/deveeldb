using System;

using Deveel.Data.Sql;

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
