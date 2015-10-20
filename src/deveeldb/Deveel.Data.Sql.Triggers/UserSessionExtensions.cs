using System;

using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Triggers {
	public static class UserSessionExtensions {
		public static void CreateTrigger(this IUserSession session, TriggerInfo triggerInfo) {
			var manager = session.Transaction.GetTriggerManager();
			if (manager == null)
				return;

			manager.CreateTrigger(triggerInfo);
		}

		public static bool TriggerExists(this IUserSession session, ObjectName triggerName) {
			var manager = session.Transaction.GetTriggerManager();
			if (manager == null)
				return false;

			return manager.TriggerExists(triggerName);
		}
	}
}
