using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Triggers {
	static class TriggerEventTypeExtensions {
		public static string AsDebugString(this TriggerEventType eventType) {
			var matched = new List<string>();
			if ((eventType & TriggerEventType.Insert) != 0)
				matched.Add("INSERT");
			if ((eventType & TriggerEventType.Update) != 0)
				matched.Add("UPDATE");
			if ((eventType & TriggerEventType.Delete) != 0)
				matched.Add("DELETE");

			return String.Join(" OR ", matched.ToArray());
		}
	}
}
