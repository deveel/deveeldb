// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

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
