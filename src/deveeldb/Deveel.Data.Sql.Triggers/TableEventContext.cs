using System;

namespace Deveel.Data.Sql.Triggers {
	/// <summary>
	/// Exposes the context of an event fired on a table.
	/// </summary>
	public sealed class TableEventContext {
		internal TableEventContext(ITable table, TriggerEventType eventType, RowId oldRowId, Row newRow) {
			if (table == null)
				throw new ArgumentNullException("table");

			Table = table;
			EventType = eventType;
			OldRowId = oldRowId;
			NewRow = newRow;
		}

		/// <summary>
		/// Gets the table on which the event occurred.
		/// </summary>
		public ITable Table { get; private set; }

		/// <summary>
		/// Gets the type of event that occurred on the table.
		/// </summary>
		public TriggerEventType EventType { get; private set; }

		/// <summary>
		/// Gets an optional reference to a row removed or updated.
		/// </summary>
		public RowId OldRowId { get; private set; }

		/// <summary>
		/// Gets the row object being added or updated.
		/// </summary>
		public Row NewRow { get; private set; }
	}
}
