using System;

using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Triggers {
	public static class QueryExtensions {
		public static void FireTriggers(this IQuery context, TableEventContext tableEvent) {
			var manager = context.Session.Transaction.GetTriggerManager();
			if (manager == null)
				return;

			manager.FireTriggers(context, tableEvent);
		}

		public static void CreateTrigger(this IQuery context, TriggerInfo triggerInfo) {
			context.Session.CreateTrigger(triggerInfo);
		}

		public static void CreateCallbackTrigger(this IQuery context, ObjectName triggerName, TriggerEventType eventType) {
			context.CreateTrigger(new TriggerInfo(triggerName, eventType));
		}

		public static bool TriggerExists(this IQuery context, ObjectName triggerName) {
			return context.Session.TriggerExists(triggerName);
		}
	}
}
