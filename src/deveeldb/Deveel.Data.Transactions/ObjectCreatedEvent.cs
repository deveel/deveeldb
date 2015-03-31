using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public class ObjectCreatedEvent : ITransactionEvent {
		public ObjectCreatedEvent(ObjectName objectName, DbObjectType objectType) {
			ObjectName = objectName;
			ObjectType = objectType;
		}

		public ObjectName ObjectName { get; private set; }

		public DbObjectType ObjectType { get; private set; }
	}
}
