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
