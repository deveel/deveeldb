using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public class ObjectDroppedEvent : ITransactionEvent {
		public ObjectDroppedEvent(DbObjectType objectType, ObjectName objectName) {
			ObjectType = objectType;
			ObjectName = objectName;
		}

		public DbObjectType ObjectType { get; private set; }

		public ObjectName ObjectName { get; private set; }
	}
}
