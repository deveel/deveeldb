using System;

namespace Deveel.Data.Sql.Triggers {
	static class TriggerEventEventTypeExtensions {
		public static string AsString(this TriggerEventType eventType) {
			string moment = null;
			string operation = null;

			if ((eventType & TriggerEventType.After) != 0) {
				moment = "AFTER";
			} else if ((eventType & TriggerEventType.Before) != 0) {
				moment = "BEFORE";
			}

			if ((eventType & TriggerEventType.Delete) != 0) {
				operation = "DELETE";
			} else if ((eventType & TriggerEventType.Insert) != 0) {
				operation = "INSERT";
			} else if ((eventType & TriggerEventType.Update) != 0) {
				operation = "UPDATE";
			}

			return String.Format("{0} {1}", moment, operation);
		}
	}
}
