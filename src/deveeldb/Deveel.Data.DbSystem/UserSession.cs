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

using Deveel.Data.Protocol;
using Deveel.Data.Security;
using Deveel.Data.Sql.Query;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class UserSession : IUserSession {
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

		public ObjectName ResolveObjectName(string name) {
			return Transaction.ResolveObjectName(name);
		}

		public bool TableExists(ObjectName tableName) {
			return Transaction.TableExists(tableName);
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
			lock (Database) {
				if (lockHandles != null) {
					foreach (var handle in lockHandles) {
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
			Transaction = null;
			Database = null;

			// TODO: fire pending events left ...

			ReleaseLocks();
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