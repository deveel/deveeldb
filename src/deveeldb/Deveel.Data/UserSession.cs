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
using System.Collections.Generic;

using Deveel.Data.Diagnostics;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// This is a session that is constructed around a given user and a transaction,
	/// to the given database.
	/// </summary>
	public sealed class UserSession : IUserSession {
		private List<LockHandle> lockHandles;

		/// <summary>
		/// Constructs the session for the given user and transaction to the
		/// given database.
		/// </summary>
		/// <param name="database">The database to which the user is connected.</param>
		/// <param name="transaction">A transaction that handles the commands issued by
		/// the user during the session.</param>
		/// <param name="sessionInfo">The information about the session to be created (user, 
		/// connection endpoint, statistics, etc.)</param>
		/// <seealso cref="ITransaction"/>
		/// <seealso cref="SessionInfo"/>
		public UserSession(IDatabase database, ITransaction transaction, SessionInfo sessionInfo) {
			if (database == null)
				throw new ArgumentNullException("database");
			if (transaction == null)
				throw new ArgumentNullException("transaction");
			if (sessionInfo == null)
				throw new ArgumentNullException("sessionInfo");

			if (sessionInfo.User.IsSystem ||
				sessionInfo.User.IsPublic)
				throw new ArgumentException(String.Format("Cannot open a session for user '{0}'.", sessionInfo.User.Name));

			Database = database;
			Transaction = transaction;

			Database.DatabaseContext.Sessions.Add(this);

			SessionInfo = sessionInfo;
		}

		~UserSession() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public string CurrentSchema {
			get { return Transaction.CurrentSchema(); }
		}

		public SessionInfo SessionInfo { get; private set; }

		void IEventSource.AppendEventData(IEvent @event) {
			@event.Database(Database.Name());
			@event.UserName(SessionInfo.User.Name);
			@event.RemoteAddress(SessionInfo.EndPoint.ToString());
		}

		public ITransaction Transaction { get; private set; }

		public void Lock(ILockable[] toWrite, ILockable[] toRead, LockingMode mode) {
			lock (Database) {
				if (lockHandles == null)
					lockHandles = new List<LockHandle>();

				var handle = Database.DatabaseContext.Locker.Lock(toWrite, toRead, mode);
				if (handle != null)
					lockHandles.Add(handle);
			}
		}

		public void ReleaseLocks() {
			if (Database == null)
				return;

			lock (Database) {
				if (lockHandles != null) {
					foreach (var handle in lockHandles) {
						if (handle != null)
							handle.Release();
					}
				}
			}
		}

		public IDatabase Database { get; private set; }

		public void Commit() {
			if (Transaction != null) {
				try {
					Transaction.Commit();
				} finally {
					SessionInfo.OnCommand();
					DisposeTransaction();
				}
			}
		}

		public void Rollback() {
			if (Transaction != null) {
				try {
					Transaction.Rollback();
				} finally {
					SessionInfo.OnCommand();
					DisposeTransaction();
				}
			}
		}

		private void DisposeTransaction() {
			ReleaseLocks();

			if (Database != null)
				Database.DatabaseContext.Sessions.Remove(this);

			Transaction = null;
			Database = null;
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				try {
					Rollback();
				} catch (Exception e) {
					// TODO: Notify the underlying system
				}
			}

			lockHandles = null;
		}
	}
}