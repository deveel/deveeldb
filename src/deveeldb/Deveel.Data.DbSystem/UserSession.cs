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
using System.Data;

using Deveel.Data.Diagnostics;
using Deveel.Data.Protocol;
using Deveel.Data.Security;
using Deveel.Data.Sql.Query;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class UserSession : IUserSession, ISessionEventSource {
		private List<LockHandle> lockHandles;

		internal UserSession(IDatabase database, ITransaction transaction, User user, ConnectionEndPoint userEndPoint) {
			Database = database;
			Transaction = transaction;
			User = user;
			EndPoint = userEndPoint;

			user.Activate(this);
		}

		~UserSession() {
			Dispose(false);
		}

		public bool IgnoreCase {
			get { return Transaction.IgnoreIdentifiersCase(); }
		}

		public ITableQueryInfo GetQueryInfo(ObjectName tableName, ObjectName givenName) {
			throw new NotImplementedException();
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public User User { get; private set; }

		public string CurrentSchema {
			get { return Transaction.CurrentSchema(); }
		}

		public ConnectionEndPoint EndPoint { get; private set; }

		public DateTimeOffset? LastCommand {
			get { throw new NotImplementedException(); }
		}

		string IDatabaseEventSource.DatabaseName {
			get { return Database.Name(); }
		}

		string ISessionEventSource.UserName {
			get { return User.Name; }
		}

		public ITransaction Transaction { get; private set; }

		public void Lock(ILockable[] toWrite, ILockable[] toRead, LockingMode mode) {
			lock (Database) {
				if (lockHandles == null)
					lockHandles = new List<LockHandle>();

				var handle = Database.Context.Locker.Lock(toWrite, toRead, mode);
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

		public ILargeObject CreateLargeObject(long size, bool compressed) {
			throw new NotImplementedException();
		}

		public ILargeObject GetLargeObject(ObjectId objId) {
			throw new NotImplementedException();
		}

		public IDbConnection GetDbConnection() {
			throw new NotImplementedException();
		}

		public void Commit() {
			if (Transaction != null) {
				try {
					Transaction.Commit();
				} finally {
					DisposeTransaction();
				}
			}
		}

		public void Rollback() {
			if (Transaction != null) {
				try {
					Transaction.Rollback();
				} finally {
					DisposeTransaction();
				}
			}
		}

		private void DisposeTransaction() {
			// TODO: fire pending events left ...

			ReleaseLocks();

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