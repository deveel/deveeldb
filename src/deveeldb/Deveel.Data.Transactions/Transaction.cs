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
		//private Action<TableCommitInfo> commitActions; 

		private readonly bool dbReadOnly;

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

		private void AssertNotReadOnly() {
			if (this.ReadOnly())
				throw new TransactionException(TransactionErrorCodes.ReadOnly, "The transaction is in read-only mode.");
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

		//public void RegisterOnCommit(Action<TableCommitInfo> action) {
		//	if (commitActions == null) {
		//		commitActions = action;
		//	} else {
		//		commitActions = Delegate.Combine(commitActions, action) as Action<TableCommitInfo>;
		//	}
		//}

		//public void UnregisterOnCommit(Action<TableCommitInfo> action) {
		//	if (commitActions != null)
		//		commitActions = Delegate.Remove(commitActions, action) as Action<TableCommitInfo>;
		//}


		private void Finish() {
			try {
				// Dispose all the table we touched
				try {
					TableManager.Dispose();

					if (Context != null)
						Context.Dispose();

				} catch (Exception ex) {
					this.OnError(ex);
				}

				//callbacks = null;
				Context = null;

				// Dispose all the objects in the transaction
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
