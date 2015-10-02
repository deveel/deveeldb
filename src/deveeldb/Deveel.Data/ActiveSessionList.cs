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

namespace Deveel.Data {
	/// <summary>
	/// Manages all the open sessions towards a single database within
	/// as system.
	/// </summary>
	public sealed class ActiveSessionList : IEnumerable<IUserSession> {
		private readonly List<IUserSession> sessions;

		/// <summary>
		/// Initializes a new instance of the <see cref="ActiveSessionList"/> class that
		/// is wrapped around a given database context.
		/// </summary>
		/// <param name="databaseContext">The database context holding the sessions.</param>
		/// <exception cref="System.ArgumentNullException">If the provided
		/// <paramref name="databaseContext">database context</paramref> object is <c>null</c>.</exception>
		public ActiveSessionList(IDatabaseContext databaseContext) {
			if (databaseContext == null)
				throw new ArgumentNullException("databaseContext");

			DatabaseContext = databaseContext;
			sessions = new List<IUserSession>();
		}

		/// <summary>
		/// Gets the database context to which the sessions point to.
		/// </summary>
		/// <value>
		/// The database context.
		/// </value>
		public IDatabaseContext DatabaseContext { get; private set; }

		/// <summary>
		/// Determines whether the specific user is active.
		/// </summary>
		/// <param name="userName">Name of the user to verify.</param>
		/// <returns></returns>
		public bool IsUserActive(string userName) {
			lock (this) {
				return sessions.Any(x => x.SessionInfo.User.Name == userName);
			}
		}

		/// <summary>
		/// Gets the count of the open connections.
		/// </summary>
		/// <value>
		/// The count of open connections.
		/// </value>
		public int Count {
			get {
				lock (this) {
					return sessions.Count;
				}
			}
		}

		/// <summary>
		/// Gets the <see cref="IUserSession"/> at the specified index.
		/// </summary>
		/// <value>
		/// The <see cref="IUserSession"/>.
		/// </value>
		/// <param name="index">The zero-based index of the session to get.</param>
		/// <returns></returns>
		public IUserSession this[int index] {
			get {
				lock (this) {
					return sessions[index];
				}
			}
		}

		/// <summary>
		/// Returns an enumerator that iterates through the collection.
		/// </summary>
		/// <returns>
		/// A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection.
		/// </returns>
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

		internal void Remove(IUserSession session) {
			lock (this) {
				sessions.Remove(session);
			}
		}
	}
}
