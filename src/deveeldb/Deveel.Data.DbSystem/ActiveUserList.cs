using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Routines;
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
