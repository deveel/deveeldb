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

using Deveel.Data.Diagnostics;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// This is a session that is constructed around a given user and a transaction,
	/// to the given database.
	/// </summary>
	public sealed class Session : ISession, IEventSource, ISystemDirectAccess {
		private bool disposed;
		private readonly DateTimeOffset startedOn;
		private SystemAccess access;

		private DateTimeOffset? lastCommandTime;
		private SqlQuery lastCommand;

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
			Context.RouteImmediate<CounterEvent>(Count);

			Transaction.GetTableManager().AddInternalTables(new SessionTableContainer(this));

			Counters = new CounterRegistry();
			access = new SessionAccess(this);

			transaction.Database.Sessions.Add(this);

			User = new User(this, userName);
			startedOn = DateTimeOffset.UtcNow;

			this.AsEventSource().OnEvent(new SessionEvent(SessionEventType.Begin));
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

		public CounterRegistry Counters { get; private set; }

		public User User { get; private set; }

		SystemAccess ISystemDirectAccess.DirectAccess {
			get { return access; }
		}

		IEventSource IEventSource.ParentSource {
			get { return Transaction.AsEventSource(); }
		}

		IContext IEventSource.Context {
			get { return Context; }
		}

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get { return GetMetadata(); }
		}

		public IDictionary<string, object> Metadata { get; set; }

		private void Count(CounterEvent e) {
			if (e.IsIncremental) {
				Counters.Increment(e.CounterKey);
			} else {
				Counters.SetValue(e.CounterKey, e.Value);
			}
		}

		private IEnumerable<KeyValuePair<string, object>> GetMetadata() {
			var meta = new Dictionary<string, object> {
				{KnownEventMetadata.UserName, User.Name},
				{KnownEventMetadata.SessionStartTime, startedOn},
				{KnownEventMetadata.LastCommandTime, lastCommandTime},
				{KnownEventMetadata.LastCommand, lastCommand != null ? lastCommand.Text : null}
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

	    public IDatabase Database {
	        get { return Transaction == null ? null : Transaction.Database; }
	    }

		private void OnQueryCommand(QueryEvent e) {
			lastCommandTime = e.TimeStamp;
			lastCommand = e.Query;
		}

		public void Commit() {
			AssertNotDisposed();

			try {
				this.AsEventSource().OnEvent(new SessionEvent(SessionEventType.BeforeCommit));

				if (Transaction != null) {
					try {
						Transaction.Commit();
					} finally {
						DisposeTransaction();
					}
				}
			} finally {
				this.AsEventSource().OnEvent(new SessionEvent(SessionEventType.AfterCommit));
			}
		}

		public void Rollback() {
			AssertNotDisposed();

			try {
				this.AsEventSource().OnEvent(new SessionEvent(SessionEventType.BeforeRollback));

				if (Transaction != null) {
					try {
						Transaction.Rollback();
					} finally {
						DisposeTransaction();
					}
				}
			} finally {
				this.AsEventSource().OnEvent(new SessionEvent(SessionEventType.AfterRollback));
			}
		}

		private void DisposeTransaction() {
			if (Database != null)
				Database.Sessions.Remove(this);

			Transaction = null;
		}

		public IQuery CreateQuery() {
			return new Query(this);
		}

		public ILargeObject CreateLargeObject(long maxSize, bool compressed) {
			var composite = Transaction.GetTableManager().Composite;
			if (composite == null)
				return null;

			return composite.CreateLargeObject(maxSize, compressed);
		}

		public ILargeObject GetLargeObject(ObjectId objectId) {
			var composite = Transaction.GetTableManager().Composite;
			if (composite == null)
				return null;

			return composite.GetLargeObject(objectId);
		}

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					try {
						Rollback();
					} catch (Exception ex) {
						this.OnError(new Exception("Error while rolling back on Dispose", ex));
					} finally {
						if (Context != null)
							Context.Dispose();
					}
				}

				Context = null;
				access = null;
				disposed = true;
			}
		}
	}
}