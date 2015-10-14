using System;

namespace Deveel.Data.Sql.Triggers {
	public class TableEventException : SqlErrorException {
		public TableEventException(TableEventContext tableEvent)
			: this(tableEvent, FormMessage(tableEvent)) {
		}

		public TableEventException(TableEventContext tableEvent, Exception innerException)
			: this(tableEvent, FormMessage(tableEvent), innerException) {
		}

		public TableEventException(TableEventContext tableEvent, string message)
			: this(tableEvent, message, null) {
		}

		public TableEventException(TableEventContext tableEvent, string message, Exception innerException)
			: base(-1, message, innerException) {
			TableName = tableEvent.Table.FullName;
			EventType = tableEvent.EventType;
		}

		public ObjectName TableName { get; private set; }

		public TriggerEventType EventType { get; private set; }

		private static string FormMessage(TableEventContext tableEvent) {
			return String.Format("An error occurred when firing triggers '{0}' on '{1}'.", tableEvent.EventType.AsString(),
				tableEvent.Table.FullName);
		}
	}
}
