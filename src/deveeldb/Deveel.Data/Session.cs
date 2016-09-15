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
using Deveel.Data.Sql.Statements;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// This is a session that is constructed around a given user and a transaction,
	/// to the given database.
	/// </summary>
	public sealed class Session : EventSource, ISession, IProvidesDirectAccess {
		private bool disposed;
		private readonly DateTimeOffset startedOn;
		private SystemAccess access;

		private DateTimeOffset? lastCommandTime;
		private SqlQuery lastCommand;
		private StatementResult[] lastCommandResult;
		private TimeSpan tzOffset;

		/// <summary>
		/// Constructs the session for the given user and transaction to the
		/// given database.
		/// </summary>
		/// <param name="transaction">A transaction that handles the commands issued by
		/// the user during the session.</param>
		/// <param name="userName"></param>
		/// <seealso cref="ITransaction"/>
		public Session(ITransaction transaction, string userName)
			: base(transaction as IEventSource) {
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

			Transaction.Context.Route<QueryEvent>(OnQueryCommand);

			Transaction.GetTableManager().AddInternalTables(new SessionTableContainer(this));

			access = new SessionAccess(this);

			if (!transaction.Database.Sessions.Add(this))
				throw new InvalidOperationException("The session was already in the database session list");

			User = new User(this, userName);
			startedOn = DateTimeOffset.UtcNow;

			this.OnEvent(new SessionEvent(SessionEventType.Begin));
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

		SystemAccess IProvidesDirectAccess.DirectAccess {
			get { return access; }
		}

		IContext IContextBased.Context {
			get { return Context; }
		}

		private int AffectedRows {
			get {
				if (lastCommandResult == null ||
					lastCommandResult.Length == 0)
					return 0;

				var result = lastCommandResult[0];
				if (result.Type != StatementResultType.Result)
					return 0;

				return result.Result.RowCount;
			}
		}

		public void SetTimeZone(int hours, int minutes) {
			tzOffset = new TimeSpan(0, hours, minutes, 0);
		}

		protected override void GetMetadata(Dictionary<string, object> metadata) {
			metadata[MetadataKeys.Session.UserName] = User.Name;
			metadata[MetadataKeys.Session.StartTimeUtc] = startedOn;
			metadata[MetadataKeys.Session.LastCommandTime] = lastCommandTime;
			metadata[MetadataKeys.Session.LastCommandText] = lastCommand != null ? lastCommand.Text : null;
			metadata[MetadataKeys.Session.LastCommandAffectedRows] = AffectedRows;
			metadata[MetadataKeys.Session.TimeZone] = tzOffset.ToString();
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
			lastCommandResult = e.Result;
		}

		public void Commit() {
			AssertNotDisposed();

			try {
				this.OnEvent(new SessionEvent(SessionEventType.BeforeCommit));

				if (Transaction != null) {
					try {
						Transaction.Commit();
					} finally {
						DisposeTransaction();
					}
				}
			} finally {
				this.OnEvent(new SessionEvent(SessionEventType.AfterCommit));
			}
		}

		public void Rollback() {
			AssertNotDisposed();

			try {
				this.OnEvent(new SessionEvent(SessionEventType.BeforeRollback));

				if (Transaction != null) {
					try {
						Transaction.Rollback();
					} finally {
						DisposeTransaction();
					}
				}
			} finally {
				this.OnEvent(new SessionEvent(SessionEventType.AfterRollback));
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