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

using Deveel.Data.Security;

namespace Deveel.Data.DbSystem {
	public sealed class ActiveUserList : IEnumerable<User> {
		private readonly List<User> users;

		public ActiveUserList(IDatabase database) {
			if (database == null)
				throw new ArgumentNullException("database");

			Database = database;
			users = new List<User>();
		}

		public IDatabase Database { get; private set; }

		public int Count {
			get {
				lock (this) {
					return users.Count;
				}
			}
		}

		public User this[int index] {
			get {
				lock (this) {
					return users[index];
				}
			}
		}
 
		public IEnumerator<User> GetEnumerator() {
			lock (this) {
				return users.GetEnumerator();
			}
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		internal bool Add(User user) {
			lock (this) {
				if (users.Contains(user))
					return false;

				users.Add(user);
				return true;
			}
		}

		internal bool Remove(User user) {
			lock (this) {
				return users.Remove(user);
			}
		}
	}
}
