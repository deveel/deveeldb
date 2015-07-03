using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Triggers {
	/// <summary>
	/// Represents the context in which a trigger is evaluated.
	/// </summary>
	public sealed class TriggerContext : IDisposable {
		internal TriggerContext(IUserSession session, ITable table, TriggerEventType eventType, RowId oldRowId, Row newRow) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
			Table = table;
			EventType = eventType;
			OldRowId = oldRowId;
			NewRow = newRow;
		}

		/// <summary>
		/// Gets the session in which a modification operation is done.
		/// </summary>
		public IUserSession Session { get; private set; }

		/// <summary>
		/// Gets the table in which the modification happened.
		/// </summary>
		public ITable Table { get; private set; }

		/// <summary>
		/// Gets the <see cref="RowId"/> of a row that was deleted or updated.
		/// </summary>
		public RowId OldRowId { get; private set; }

		/// <summary>
		/// Gets the <see cref="Row"/> that was inserted or updated.
		/// </summary>
		public Row NewRow { get; private set; }

		/// <summary>
		/// Gets the type of event that generated the context.
		/// </summary>
		public TriggerEventType EventType { get; private set; }

		public void Dispose() {
			Session = null;
			Table = null;
		}
	}
}
