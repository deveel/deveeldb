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
