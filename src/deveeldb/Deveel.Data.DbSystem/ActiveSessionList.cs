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
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Security;

namespace Deveel.Data.DbSystem {
	public sealed class ActiveSessionList : IEnumerable<IUserSession> {
		private readonly List<IUserSession> sessions;

		public ActiveSessionList(IDatabase database) {
			if (database == null)
				throw new ArgumentNullException("database");

			Database = database;
			sessions = new List<IUserSession>();
		}

		public IDatabase Database { get; private set; }

		public bool IsUserActive(string userName) {
			lock (this) {
				return sessions.Any(x => x.SessionInfo.User.Name == userName);
			}
		}

		public int Count {
			get {
				lock (this) {
					return sessions.Count;
				}
			}
		}

		public IUserSession this[int index] {
			get {
				lock (this) {
					return sessions[index];
				}
			}
		}
 
		public IEnumerator<IUserSession> GetEnumerator() {
			lock (this) {
				return sessions.GetEnumerator();
			}
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		internal bool Add(IUserSession session) {
			lock (this) {
				if (sessions.Contains(session))
					return false;

				sessions.Add(session);
				return true;
			}
		}
	}
}
