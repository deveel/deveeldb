using System;

using Deveel.Data.Diagnostics;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Triggers {
	public static class QueryContextExtensions {
		public static void FireTriggers(this IQueryContext context, TableEventContext tableEvent) {
			var manager = context.Session.Transaction.GetTriggerManager();
			if (manager == null)
				return;

			manager.FireTriggers(context, tableEvent);
		}

		//public static void FireTrigger(this IQueryContext context, TableEventContext tableEvent) {
		//	var tableName = tableEvent.Table.FullName;
		//	var eventType = tableEvent.EventType;

		//	try {
		//		var triggers = context.Session.FindTriggers(tableName, eventType);

		//		foreach (var trigger in triggers) {
		//			try {
		//				trigger.Invoke(tableEvent);

		//				var oldRowId = tableEvent.OldRowId;
		//				var newRow = tableEvent.NewRow;

		//				context.FireTrigger(trigger.TriggerName, tableName, eventType, oldRowId, newRow);
		//			} catch (Exception ex) {
		//				context.RegisterTriggerError(trigger, ex);
		//			}
		//		}
		//	} catch (TableEventException ex) {
		//		context.RegisterError(ex);
		//		throw;
		//	} catch (Exception ex) {
		//		context.RegisterTableEventError(tableEvent, ex);
		//		throw new TableEventException(tableEvent, ex);
		//	}
		//}

		//private static void RegisterTriggerError(this IQueryContext context, Trigger trigger, Exception error) {
		//	context.RegisterError(new TriggerException(trigger, error));
		//}

		//private static void RegisterTableEventError(this IQueryContext context, TableEventContext @event, Exception error) {
		//	context.RegisterError(new TableEventException(@event, error));
		//}

		public static void CreateTrigger(this IQueryContext context, TriggerInfo triggerInfo) {
			context.Session.CreateTrigger(triggerInfo);
		}

		public static void CreateCallbackTrigger(this IQueryContext context, ObjectName triggerName, TriggerEventType eventType) {
			context.CreateTrigger(new TriggerInfo(triggerName, eventType));
		}

		public static bool TriggerExists(this IQueryContext context, ObjectName triggerName) {
			return context.Session.TriggerExists(triggerName);
		}
    }
}
