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
using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// The system implementation of a transaction model that handles
	/// isolated operations within a database context.
	/// </summary>
	/// <seealso cref="ITransaction"/>
	public sealed class Transaction : ITransaction, IEventSource, ITableStateHandler {
		private List<LockHandle> lockHandles;

		internal Transaction(ITransactionContext context, Database database, int commitId, IsolationLevel isolation, IEnumerable<TableSource> committedTables, IEnumerable<IIndexSet> indexSets) {
			CommitId = commitId;
			Database = database;
			Isolation = isolation;
		    Context = context;

			context.RegisterInstance<ITransaction>(this);

			TableManager.AddVisibleTables(committedTables, indexSets);

			AddInternalTables();

			TableState = new OldNewTableState();

			IsClosed = false;

			Database.TransactionFactory.OpenTransactions.AddTransaction(this);

			State = TransactionState.Open;

			this.CurrentSchema(database.Context.DefaultSchema());
			this.ReadOnly(database.Context.ReadOnly());
			this.AutoCommit(database.Context.AutoCommit());
			this.IgnoreIdentifiersCase(database.Context.IgnoreIdentifiersCase());
			this.ParameterStyle(QueryParameterStyle.Marker);

			this.AsEventSource().OnEvent(new TransactionEvent(commitId, TransactionEventType.Begin));
		}

		internal Transaction(ITransactionContext context, Database database, int commitId, IsolationLevel isolation)
			: this(context, database, commitId, isolation, new TableSource[0], new IIndexSet[0]) {
		}

		~Transaction() {
			Dispose(false);
		}

		public int CommitId { get; private set; }

		public IsolationLevel Isolation { get; private set; }

		public TransactionState State { get; private set; }

		private bool IsClosed { get; set; }

		public OldNewTableState TableState { get; private set; }

        public ITransactionContext Context { get; private set; }

		public void SetTableState(OldNewTableState tableState) {
			TableState = tableState;
		}

		IDatabase ITransaction.Database {
			get { return Database; }
		}

		public Database Database { get; private set; }

		IEventSource IEventSource.ParentSource {
			get { return Database.AsEventSource(); }
		}

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get { return new Dictionary<string, object> {
				{ KnownEventMetadata.CommitId, CommitId },
				{ KnownEventMetadata.IgnoreIdentifiersCase, this.IgnoreIdentifiersCase() },
				{ KnownEventMetadata.IsolationLevel, Isolation },
				{ KnownEventMetadata.CurrentSchema, this.CurrentSchema() },
                { KnownEventMetadata.ReadOnlyTransaction, this.ReadOnly() }
			};}
		}
			
		IContext IEventSource.Context {
			get { return Context; }
		}

		private TableSourceComposite TableComposite {
			get { return Database.TableComposite; }
		}

		public TransactionRegistry Registry {
			get { return ((TransactionContext) Context).EventRegistry; }
		}

		private TableManager TableManager {
			get { return this.GetTableManager(); }
		}

		private void AddInternalTables() {
			var tableContainers = Context.ResolveAllServices<ITableContainer>();
			foreach (var container in tableContainers) {
				TableManager.AddInternalTables(container);
			}

			TableManager.AddInternalTables(new TransactionTableContainer(this));
		}

		private void ReleaseLocks() {
			if (Database == null)
				return;

			lock (Database) {
				if (lockHandles != null) {
					foreach (var handle in lockHandles) {
						if (handle != null) {
							Database.Locker.Unlock(handle);
						}
					}

					lockHandles.Clear();
				}

				lockHandles = null;
			}
		}

		public void Lock(IEnumerable<IDbObject> objects, LockingMode mode, int timeout) {
			lock (Database) {
				var lockables = objects.OfType<ILockable>().ToArray();
				if (lockables.Length == 0)
					return;

				// Before we can lock the objects, we must wait for them
				//  to be available...
				if (lockables.Any(x => Database.Locker.IsLocked(x)))
					Database.Locker.CheckAccess(lockables, AccessType.ReadWrite, timeout);

				var handle = Database.Locker.Lock(lockables, AccessType.ReadWrite, mode);

				if (lockHandles == null)
					lockHandles = new List<LockHandle>();

				lockHandles.Add(handle);

				var lockedNames = objects.Where(x => x is ILockable).Select(x => x.ObjectInfo.FullName);
				Context.OnEvent(new LockEvent(LockEventType.Lock, lockedNames, LockingMode.Exclusive, AccessType.ReadWrite));
			}
		}

		public void Enter(IEnumerable<IDbObject> objects, AccessType accessType) {
			if (Database == null)
				return;

			lock (Database) {
				var lockables = objects.OfType<ILockable>().ToArray();
				if (lockables.Length == 0)
					return;

				var timeout = this.LockTimeout();

				if (lockables.Any(x => Database.Locker.IsLocked(x))) {
					if (Isolation == IsolationLevel.ReadCommitted) {
						Database.Locker.CheckAccess(lockables, AccessType.Read, timeout);
					} else if (Isolation == IsolationLevel.Serializable) {
						Database.Locker.CheckAccess(lockables, AccessType.ReadWrite, timeout);
					}
				}

				var handle = Database.Locker.Lock(lockables, AccessType.ReadWrite, LockingMode.Exclusive);

				var tables = lockables.OfType<IDbObject>().Where(x => x.ObjectInfo.ObjectType == DbObjectType.Table)
					.Select(x => x.ObjectInfo.FullName);
				foreach (var table in tables) {
					TableManager.SelectTable(table);
				}

				if (handle != null) {
					if (lockHandles == null)
						lockHandles = new List<LockHandle>();

					lockHandles.Add(handle);
				}

				var lockedNames = objects.Where(x => x is ILockable).Select(x => x.ObjectInfo.FullName);
				Context.OnEvent(new LockEvent(LockEventType.Enter, lockedNames, LockingMode.Exclusive, accessType));
			}
		}

		public void Exit(IEnumerable<IDbObject> objects, AccessType accessType) {
			if (Database == null)
				return;

			lock (Database) {
				var lockables = objects.OfType<ILockable>().ToArray();
				if (lockables.Length == 0)
					return;

				if (lockHandles != null) {
					for (int i = lockables.Length - 1; i >= 0; i--) {
						var handle = lockHandles[i];

						bool handled = true;
						foreach (var lockable in lockables) {
							if (!handle.IsHandled(lockable)) {
								handled = false;
								break;
							}
						}

						if (handled) {
							Database.Locker.Unlock(handle);
							lockHandles.RemoveAt(i);
						}
					}
				}

				var lockedNames = objects.Where(x => x is ILockable).Select(x => x.ObjectInfo.FullName);
				Context.OnEvent(new LockEvent(LockEventType.Exit, lockedNames, LockingMode.Exclusive, accessType));
			}
		}


		public void Commit() {
			if (!IsClosed) {
				try {
					State = TransactionState.Commit;

					var touchedTables = TableManager.AccessedTables.ToList();
					var visibleTables = TableManager.GetVisibleTables().ToList();
					var selected = TableManager.SelectedTables.ToArray();
					TableComposite.Commit(this, visibleTables, selected, touchedTables, Registry);

					this.OnEvent(new TransactionEvent(CommitId, TransactionEventType.Commit));
				} finally {
					Finish();
				}
			}
		}


		private void Finish() {
			try {
				// Dispose all the table we touched
				try {
					ReleaseLocks();

					if (TableManager != null)
						TableManager.Dispose();

					if (Context != null)
						Context.Dispose();

				} catch (Exception ex) {
					this.OnError(ex);
				}

				Context = null;
			} finally {
				IsClosed = true;
				State = TransactionState.Finished;
			}
		}

		public void Rollback() {
			if (!IsClosed) {
				try {
					State = TransactionState.Rollback;

					var touchedTables = TableManager.AccessedTables.ToList();
					TableComposite.Rollback(this, touchedTables, Registry);

					this.OnEvent(new TransactionEvent(CommitId, TransactionEventType.Rollback));
				} finally {
					IsClosed = true;
					Finish();
				}
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				//if (!IsClosed)
				//	Rollback();
				// TODO: review this ...
				Finish();
			}
		}
	}
}
