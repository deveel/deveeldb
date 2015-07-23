using System;
using System.Diagnostics;

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
