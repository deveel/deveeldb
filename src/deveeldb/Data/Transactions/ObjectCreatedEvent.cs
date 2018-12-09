using System;
using System.Collections.Generic;

using Deveel.Data.Events;
using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public class ObjectCreatedEvent : Event {
		public ObjectCreatedEvent(IEventSource source, DbObjectType objectType, ObjectName objectName) : base(source) {
			if (ObjectName.IsNullOrEmpty(objectName))
				throw new ArgumentNullException(nameof(objectName));

			ObjectType = objectType;
			ObjectName = objectName;
		}

		public DbObjectType ObjectType { get; }

		public ObjectName ObjectName { get; }

		protected override void GetEventData(IDictionary<string, object> data) {
			data["obj.type"] = ObjectType.ToString().ToLowerInvariant();
			data["obj.name"] = ObjectName.ToString();
		}
	}
}