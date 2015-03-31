using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class TableCreatedEvent : ObjectCreatedEvent, ITableEvent {
		public TableCreatedEvent(int tableId, ObjectName tableName)
			: base(tableName, DbObjectType.Table) {
			TableId = tableId;
		}

		public int TableId { get; private set; }
	}
}
