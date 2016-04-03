// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Diagnostics {
	public static class DatabaseExtensions {
		public static void OnSessionEvent(this IDatabase database, string userName, int commitId, SessionEventType eventType) {
			database.AsEventSource().OnEvent(new SessionEvent(userName, commitId, eventType));
		}

		public static void OnSessionBegin(this IDatabase database, string userName, int commitId) {
			database.OnSessionEvent(userName, commitId, SessionEventType.Begin);
		}

		public static void OnSessionCommit(this IDatabase database, string userName, int commitId) {
			database.OnSessionEvent(userName, commitId, SessionEventType.EndForCommit);
		}

		public static void OnSessionRollback(this IDatabase database, string userName, int commitId) {
			database.OnSessionEvent(userName, commitId, SessionEventType.EndForRollback);
		}
	}
}
