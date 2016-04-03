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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Diagnostics;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// This is a session that is constructed around a given user and a transaction,
	/// to the given database.
	/// </summary>
	public sealed class Session : ISession, ISystemDirectAccess {
		private List<LockHandle> lockHandles;
		private bool disposed;
		private readonly DateTimeOffset startedOn;
		private SystemAccess access;

		/// <summary>
		/// Constructs the session for the given user and transaction to the
		/// given database.
		/// </summary>
		/// <param name="transaction">A transaction that handles the commands issued by
		/// the user during the session.</param>
		/// <param name="userName"></param>
		/// <seealso cref="ITransaction"/>
		public Session(ITransaction transaction, string userName) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");
			
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			if (String.Equals(userName, User.SystemName, StringComparison.OrdinalIgnoreCase) || 
				String.Equals(userName, User.PublicName, StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(String.Format("Cannot open a session for user '{0}'.", userName));

            Transaction = transaction;
		    Context = transaction.Context.CreateSessionContext();
			Context.RegisterInstance(this);
			Context.Route<QueryEvent>(OnQueryCommand);

			access = new SessionAccess(this);

			transaction.Database.Sessions.Add(this);

			User = new User(this, userName);
			startedOn = DateTimeOffset.UtcNow;
		}

		~Session() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public string CurrentSchema {
			get { return Transaction.CurrentSchema(); }
		}

	    public ISessionContext Context { get; private set; }

		public User User { get; private set; }

		SystemAccess ISystemDirectAccess.DirectAccess {
			get { return access; }
		}

		IEventSource IEventSource.ParentSource {
			get { return Transaction; }
		}

		IContext IEventSource.Context {
			get { return Context; }
		}

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get { return GetMetadata(); }
		}

		public IDictionary<string, object> Metadata { get; set; }

		private IEnumerable<KeyValuePair<string, object>> GetMetadata() {
			var meta = new Dictionary<string, object> {
				{KnownEventMetadata.UserName, User.Name},
				{ KnownEventMetadata.SessionStartTime, startedOn }
			};

			if (Metadata != null) {
				foreach (var pair in Metadata) {
					var key = pair.Key;
					if (!key.StartsWith("session"))
						key = String.Format("session.{0}", pair.Key);

					meta[key] = pair.Value;
				}
			}

			return meta;
		}

		public ITransaction Transaction { get; private set; }

		private void AssertNotDisposed() {
			if (disposed)
				throw new ObjectDisposedException("Session");
		}

		public void Enter(IEnumerable<IDbObject> objects, AccessType accessType) {
			if (Database == null)
				return;

			lock (Database) {
				var lockables = objects.OfType<ILockable>().ToArray();
				if (lockables.Length == 0)
					return;

				CheckAccess(lockables, accessType);

				var isolation = Transaction.Isolation;

				LockHandle handle;

				if (isolation == IsolationLevel.Serializable) {
					handle = Database.Locker.Lock(lockables, AccessType.ReadWrite, LockingMode.Exclusive);
				} else {
					throw new NotImplementedException(string.Format("The locking for isolation '{0}' is not implemented yet.", isolation));
				}

				if (handle != null) {
					if (lockHandles == null)
						lockHandles = new List<LockHandle>();

					lockHandles.Add(handle);
				}
			}
		}

		public void Exit(IEnumerable<IDbObject> objects, AccessType accessType) {
			// Only SERIALIZABLE isolation is supported, that means locks for read and write
			//    are acquired on access and released only at the end of the session/transaction
			throw new NotImplementedException("The Exit mechanism is not implemented");
		}

		public void Lock(IEnumerable<IDbObject> objects, AccessType accessType, LockingMode mode) {
			lock (Database) {
				var lockables = objects.OfType<ILockable>().ToArray();
				if (lockables.Length == 0)
					return;

				// Before we can lock the objects, we must wait for them
				//  to be available...
				CheckAccess(lockables, accessType);

				var handle = Database.Locker.Lock(lockables, accessType, mode);

				if (lockHandles == null)
					lockHandles = new List<LockHandle>();

				lockHandles.Add(handle);
			}
		}

		private void CheckAccess(ILockable[] lockables, AccessType accessType) {
			if (lockHandles == null || lockables == null)
				return;

			foreach (var handle in lockHandles) {
				foreach (var lockable in lockables) {
					if (handle.IsHandled(lockable))
						handle.CheckAccess(lockable, accessType);
				}
			}
		}

		private void ReleaseLocks() {
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

	    public IDatabase Database {
	        get { return Transaction.Database; }
	    }

		private void OnQueryCommand(QueryEvent e) {
			if (Metadata == null)
				Metadata = new Dictionary<string, object>();

			Metadata[KnownEventMetadata.LastCommandTime] = e.TimeStamp;
			Metadata[KnownEventMetadata.LastCommand] = e.Query.Text;
		}

		public void Commit() {
			AssertNotDisposed();

			if (Transaction != null) {
				try {
					Transaction.Commit();
				} finally {
					DisposeTransaction();
				}
			}
		}

		public void Rollback() {
			AssertNotDisposed();

			if (Transaction != null) {
				try {
					Transaction.Rollback();
				} finally {
					DisposeTransaction();
				}
			}
		}

		private void DisposeTransaction() {
			ReleaseLocks();

			if (Database != null)
				Database.Sessions.Remove(this);

			Transaction = null;
		}

		public IQuery CreateQuery() {
			return new Query(this);
		}

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					try {
						Rollback();
					} catch (Exception ex) {
						this.OnError(new Exception("Error while rolling back on Dispose", ex));
					}
				}

				access = null;
				lockHandles = null;
				disposed = true;
			}
		}
	}
}