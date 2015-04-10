using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Index;
using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class TransactionState {
		public TransactionState(ITransaction transaction) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			Transaction = transaction;
		}

		public ITransaction Transaction { get; private set; }

		public TransactionRegistry Registry {
			get { return Transaction.Registry; }
		}

		internal IEnumerable<TableSource> VisibleTables {
			get { return Transaction.GetVisibleTables(); }
		}

		public IEnumerable<int> SelectedTables {
			get { return Registry.SelectedTables; }
		}

		public IEnumerable<int> TouchedTables {
			get { return Registry.TouchedTables; }
		}

		public bool HasChanges {
			get {
				return Registry.TablesCreated.Any() ||
				       Registry.TablesDropped.Any() ||
				       Registry.TablesConstraintAltered.Any() ||
				       Registry.TablesChanged.Any();
			}
		}

		private TableSourceComposite TableComposite {
			get { return Transaction.Context.Database.TableComposite; }
		}

		internal IEnumerable<TableSource> SelectedTableSources {
			get { return SelectedTables == null ? new TableSource[0] : SelectedTables.Select(GetTableSource); }
		}

		internal IEnumerable<TableSource> TouchedTableSources {
			get { return TouchedTables == null ? new TableSource[0] : TouchedTables.Select(GetTableSource); }
		} 

		private TableSource GetTableSource(int tableId) {
			return Transaction.Context.Database.TableComposite.GetTableSource(tableId);
		}

		internal IEnumerable<int> Commit(IList<TransactionObjectState> objectStates, Action<ObjectName, IEnumerable<int>, IEnumerable<int>> modificationAction) {
			var changedTableList = new List<int>();

			// Get individual journals for updates made to tables in this
			// transaction.

			var changeRegistries = Registry.TableChangeRegistries.ToList();

			// The list of tables created by this registry.
			var createdTables = Registry.TablesCreated.ToList();
			// Ths list of tables dropped by this registry.
			var droppedTables = Registry.TablesDropped.ToList();
			// The list of tables that constraints were alter by this registry
			var constraintAlteredTables = Registry.TablesConstraintAltered;

			var selectedTables = Registry.SelectedTables.ToArray();

			// Get the list of all database objects that were created in the
			// transaction.
			var objectsCreated = Registry.ObjectsCreated.ToList();
			// Get the list of all database objects that were dropped in the
			// transaction.
			var objectsDropped = Registry.ObjectsDropped.ToList();

			var commitId = Transaction.CommitId;

			// This is a transaction that will represent the view of the database
			// at the end of the commit
			ITransaction checkTransaction = null;

			bool entriesCommitted = false;

			try {
				// ---- Commit check stage ----
				var conflictResolve = new ConflictResolveInfo((int) commitId, objectStates) {
					SelectedTables = selectedTables,
					CreatedTables = createdTables,
					DroppedTables = droppedTables,
					ChangedTables = changeRegistries,
					ObjectsCreated = objectsCreated,
					ObjectsDropped = objectsDropped
				};

				CheckConflicts(conflictResolve);

				var normalizedChangedTables = GetNormalizedChangedTables(createdTables, droppedTables);
				var normalizedDroppedTables = GetNormalizedDroppedTables(createdTables, droppedTables);

				// We now need to create a ITransaction object that we
				// use to send to the triggering mechanism.  This
				// object represents a very specific view of the
				// transaction.  This view contains the latest version of changed
				// tables in this transaction.  It also contains any tables that have
				// been created by this transaction and does not contain any tables
				// that have been dropped.  Any tables that have not been touched by
				// this transaction are shown in their current committed state.
				// To summarize - this view is the current view of the database plus
				// any modifications made by the transaction that is being committed.

				// How this works - All changed tables are merged with the current
				// committed table.  All created tables are added into check_transaction
				// and all dropped tables are removed from check_transaction.  If
				// there were no other changes to a table between the time the
				// transaction was created and now, the view of the table in the
				// transaction is used, otherwise the latest changes are merged.

				// Note that this view will be the view that the database will
				// ultimately become if this transaction successfully commits.  Also,
				// you should appreciate that this view is NOT exactly the same as
				// the current trasaction view because any changes that have been
				// committed by concurrent transactions will be reflected in this view.

				// Create a new transaction of the database which will represent the
				// committed view if this commit is successful.
				checkTransaction = TableComposite.CreateTransaction(TransactionIsolation.Serializable);

				// Overwrite this view with tables from this transaction that have
				// changed or have been added or dropped.

				// (Note that order here is important).  First drop any tables from
				// this view.
				foreach (var source in normalizedDroppedTables) {
					// Drop this table in the current view
					checkTransaction.RemoveVisibleTable(source);
				}

				// Now add any changed tables to the view.

				// Represents view of the changed tables
				var changedTableSource = FindChangedTables(checkTransaction, normalizedChangedTables);

				// The 'checkTransaction' now represents the view the database will be
				// if the commit succeeds.  We Lock 'checkTransaction' so it is
				// Read-only (the view is immutable).
				checkTransaction.ReadOnly(true);

				CheckConstraintViolations(checkTransaction, normalizedChangedTables, changedTableSource, constraintAlteredTables);

				// Deferred trigger events.
				FireChangeEvents(normalizedChangedTables, modificationAction);

				// NOTE: This isn't as fail safe as it could be.  We really need to
				//  do the commit in two phases.  The first writes updated indices to
				//  the index files.  The second updates the header pointer for the
				//  respective table.  Perhaps we can make the header update
				//  procedure just one file Write.

				// Finally, at this point all constraint checks have passed and the
				// changes are ready to finally be committed as permanent changes
				// to the conglomerate.  All that needs to be done is to commit our
				// IIndexSet indices for each changed table as final.
				// ISSUE: Should we separate the 'committing of indexes' changes and
				//   'committing of delete/add flags' to make the FS more robust?
				//   It would be more robust if all indexes are committed in one go,
				//   then all table flag data.

				// Set flag to indicate we have committed entries.
				entriesCommitted = true;

				// For each change to each table,
				foreach (var tableInfo in normalizedChangedTables) {
					// Get the journal that details the change to the table.
					var changeJournal = tableInfo.EventRegistry;
					if (changeJournal != null) {
						// Get the master table with this table id.
						var master = tableInfo.TableSource;

						// Commit the changes to the table.
						// We use 'commit_id' which is the current commit level we are
						// at.
						master.CommitTransactionChange(TableComposite.CurrentCommitId, changeJournal, tableInfo.IndexSet);
						
						changedTableList.Add(master.TableId);
					}
				}

				// Only do this if we've created or dropped tables.
				if (createdTables.Any() || droppedTables.Any()) {
					// Update the committed tables in the conglomerate state.
					// This will update and synchronize the headers in this conglomerate.
					TableComposite.CommitToTables(createdTables, droppedTables);
				}

				// Update the namespace clash list
				if (objectsCreated.Any() || objectsDropped.Any()) {
					objectStates.Add(new TransactionObjectState(commitId, objectsCreated, objectsDropped));
				}
			} finally {
				try {
					// If entries_committed == false it means we didn't get to a point
					// where any changed tables were committed.  Attempt to rollback the
					// changes in this transaction if they haven't been committed yet.
					if (!entriesCommitted) {
						// For each change to each table,
						foreach (var registry in changeRegistries) {
							// The table the changes were made to.
							int tableId = registry.TableId;

							// Get the master table with this table id.
							var source = TableComposite.GetTableSource(tableId);

							// Commit the rollback on the table.
							source.RollbackTransactionChange(registry);
						}
					}
				} finally {
					try {
						// Dispose the 'checkTransaction'
						if (checkTransaction != null) {
							checkTransaction.Dispose();
							TableComposite.CloseTransaction(checkTransaction);
						}

						// Always ensure a transaction close, even if we have an exception.
						// Notify the conglomerate that this transaction has closed.
						TableComposite.CloseTransaction(Transaction);
					} catch (Exception e) {
						// TODO: Rise the error ...
					}
				}
			}

			return changedTableList.AsReadOnly();
		}

		private void FireChangeEvents(IEnumerable<CommitTableInfo> changedTables, Action<ObjectName, IEnumerable<int>, IEnumerable<int>> modificationAction) {
			// For each changed table.
			foreach (var tableInfo in changedTables) {
				// Get the journal that details the change to the table.
				var changeJournal = tableInfo.EventRegistry;
				if (changeJournal != null) {
					// Get the table name
					var tableName = tableInfo.TableSource.TableName;

					modificationAction(tableName, tableInfo.AddedRows, tableInfo.RemovedRows);
				}
			}
		}

		private ITable[] FindChangedTables(ITransaction checkTransaction, CommitTableInfo[] normalizedChangedTables) {
			var changedTableSource = new ITable[normalizedChangedTables.Length];

			// Set up the above arrays

			for (int i = 0; i < normalizedChangedTables.Length; ++i) {
				// Get the information for this changed table
				var tableInfo = normalizedChangedTables[i];

				// Get the table that changed from the normalized list.
				var source = tableInfo.TableSource;

				// Did this table change since the transaction started?
				if (!tableInfo.HasChangesSinceCommit) {
					// No changes so we can pick the correct IIndexSet from the current
					// transaction.

					// Get the state of the changed tables from the Transaction
					var mtable = Transaction.GetMutableTable(source.TableName);

					// Get the current index set of the changed table
					tableInfo.IndexSet = Transaction.GetIndexSetForTable(source);

					// Flush all index changes in the table
					mtable.FlushIndexes();

					// Set the 'check_transaction' object with the latest version of the
					// table.
					checkTransaction.UpdateVisibleTable(tableInfo.TableSource, tableInfo.IndexSet);
				} else {
					// There were changes so we need to merge the changes with the
					// current view of the table.

					// It's not immediately obvious how this merge update works, but
					// basically what happens is we WriteByte the table journal with all the
					// changes into a new IMutableTableDataSource of the current
					// committed state, and then we flush all the changes into the
					// index and then update the 'check_transaction' with this change.

					// Create the IMutableTableDataSource with the changes from this
					// journal.
					var mtable = source.CreateTableAtCommit(checkTransaction, tableInfo.EventRegistry);

					// Get the current index set of the changed table
					tableInfo.IndexSet = checkTransaction.GetIndexSetForTable(source);

					// Flush all index changes in the table
					mtable.FlushIndexes();

					// Dispose the table
					mtable.Dispose();
				}

				// And now refresh the 'changedTableSource' entry
				changedTableSource[i] = checkTransaction.GetTable(source.TableName);
			}

			return changedTableSource;
		}

		private void CheckConstraintViolations(ITransaction checkTransaction, CommitTableInfo[] normalizedChangedTables, ITable[] changedTables, IEnumerable<int> constraintAltered) {
			// Any tables that the constraints were altered for we need to check
			// if any rows in the table violate the new constraints.
			foreach (var tableId in constraintAltered) {
				// We need to check there are no constraint violations for all the
				// rows in the table.
				for (int n = 0; n < normalizedChangedTables.Length; ++n) {
					CommitTableInfo tableInfo = normalizedChangedTables[n];
					if (tableInfo.TableSource.TableId == tableId) {
						checkTransaction.CheckAddConstraintViolations(changedTables[n], ConstraintDeferrability.InitiallyDeferred);
					}
				}
			}

			// For each changed table we must determine the rows that
			// were deleted and perform the remove constraint checks on the
			// deleted rows.  Note that this happens after the records are
			// removed from the index.

			// For each changed table,
			for (int i = 0; i < normalizedChangedTables.Length; ++i) {
				var tableInfo = normalizedChangedTables[i];
				// Get the registry that details the change to the table.
				var registry = tableInfo.EventRegistry;
				if (registry != null) {
					var normalizedRemovedRows = registry.RemovedRows.ToArray();
					var normalizedAddedRows = registry.AddedRows.ToArray();

					// Check removing any of the data doesn't cause a constraint violation.
					checkTransaction.CheckRemoveConstraintViolations(changedTables[i], normalizedRemovedRows, ConstraintDeferrability.InitiallyDeferred);

					// Check adding any of the data doesn't cause a constraint violation.
					checkTransaction.CheckAddConstraintViolations(changedTables[i], normalizedAddedRows, ConstraintDeferrability.InitiallyDeferred);

					// Set up the list of added and removed rows
					tableInfo.AddedRows = normalizedAddedRows;
					tableInfo.RemovedRows = normalizedRemovedRows;
				}
			}
		}

		private void AssertNoDirtySelect(int commitId, IEnumerable<int> selectedTables) {
			// We only perform this check if transaction error on dirty selects
			// are enabled.
			if (Transaction.ErrorOnDirtySelect()) {
				// For each table that this transaction selected from, if there are
				// any committed changes then generate a transaction error.
				foreach (var tableId in selectedTables) {
					var selectedTable = TableComposite.GetTableSource(tableId);
					// Find all committed journals equal to or greater than this
					// transaction's commit_id.
					var journalsSince = selectedTable.FindChangesSinceCmmit(commitId);
					if (journalsSince.Any()) {
						// Yes, there are changes so generate transaction error and
						// rollback.
						throw new TransactionException(
							TransactionErrorCodes.DirtySelect,
							"Concurrent Serializable Transaction Conflict(4): " +
							"Select from table that has committed changes: " +
							selectedTable.TableName);
					}
				}
			}
		}

		private void CheckConflicts(ConflictResolveInfo resolveInfo) {
			AssertNoDirtySelect(resolveInfo.CommitId, resolveInfo.SelectedTables);

			// Check there isn't a namespace clash with database objects.
			// We need to create a list of all create and drop activity in the
			// conglomerate from when the transaction started.
			var allDroppedObs = new List<ObjectName>();
			var allCreatedObs = new List<ObjectName>();
			foreach (var state in resolveInfo.ObjectStates) {
				if (state.CommitId >= resolveInfo.CommitId) {
					allDroppedObs.AddRange(state.DroppedObjects);
					allCreatedObs.AddRange(state.CreatedObjects);
				}
			}

			// The list of all dropped objects since this transaction
			// began.
			bool conflict5 = false;
			object conflictName = null;
			string conflictDesc = "";
			foreach (var droppedOb in allDroppedObs) {
				if (resolveInfo.ObjectsDropped.Contains(droppedOb)) {
					conflict5 = true;
					conflictName = droppedOb;
					conflictDesc = "Drop Clash";
				}
			}
			// The list of all created objects since this transaction
			// began.
			foreach (var createdOb in allCreatedObs) {
				if (resolveInfo.ObjectsCreated.Contains(createdOb)) {
					conflict5 = true;
					conflictName = createdOb;
					conflictDesc = "Create Clash";
				}
			}
			if (conflict5) {
				// Namespace conflict...
				throw new TransactionException(
					TransactionErrorCodes.DuplicateTable,
					"Concurrent Serializable Transaction Conflict(5): " +
					"Namespace conflict: " + conflictName + " " +
					conflictDesc);
			}

			// For each journal,
			foreach (var changeJournal in resolveInfo.ChangedTables) {
				// The table the change was made to.
				int tableId = changeJournal.TableId;
				// Get the master table with this table id.
				var master = TableComposite.GetTableSource(tableId);

				// True if the state contains a committed resource with the given name
				bool committedResource = TableComposite.ContainsVisibleResource(tableId);

				// Check this table is still in the committed tables list.
				if (!resolveInfo.CreatedTables.Contains(tableId) && !committedResource) {
					// This table is no longer a committed table, so rollback
					throw new TransactionException(
						TransactionErrorCodes.TableDropped,
						"Concurrent Serializable Transaction Conflict(2): " +
						"Table altered/dropped: " + master.TableName);
				}

				// Since this journal was created, check to see if any changes to the
				// tables have been committed since.
				// This will return all journals on the table with the same commit_id
				// or greater.
				var journalsSince = master.FindChangesSinceCmmit(resolveInfo.CommitId);

				// For each journal, determine if there's any clashes.
				foreach (var tableJournal in journalsSince) {
					// This will thrown an exception if a commit classes.
					changeJournal.TestCommitClash(master.TableInfo, tableJournal);
				}
			}

			// Look at the transaction journal, if a table is dropped that has
			// journal entries since the last commit then we have an exception
			// case.
			foreach (int tableId in resolveInfo.DroppedTables) {
				// Get the master table with this table id.
				var master = TableComposite.GetTableSource(tableId);
				// Any journal entries made to this dropped table?
				if (master.FindChangesSinceCmmit(resolveInfo.CommitId).Any()) {
					// Oops, yes, rollback!
					throw new TransactionException(
						TransactionErrorCodes.TableRemoveClash,
						"Concurrent Serializable Transaction Conflict(3): " +
						"Dropped table has modifications: " + master.TableName);
				}
			}
		}

		private CommitTableInfo[] GetNormalizedChangedTables(IEnumerable<int> created, IEnumerable<int> dropped) {
			// Create a normalized list of MasterTableDataSource of all tables that
			// were either changed (and not dropped), and created (and not dropped).
			// This list represents all tables that are either new or changed in
			// this transaction.

			var normalizedChangedTables = new List<CommitTableInfo>(8);

			var droppedTables = dropped.ToList();
			var createdTables = created.ToList();

			// Add all tables that were changed and not dropped in this transaction.
			foreach (var tableRegistry in Registry.TableChangeRegistries) {
				if (!droppedTables.Contains(tableRegistry.TableId)) {
					var source = TableComposite.GetTableSource(tableRegistry.TableId);
					var changes = source.FindChangesSinceCmmit(Transaction.CommitId);
					normalizedChangedTables.Add(new CommitTableInfo(source, tableRegistry, changes));
				}
			}

			// Add all tables that were created and not dropped in this transaction.
			foreach (var tableId in createdTables) {
				// If this table is not dropped in this transaction then this is a
				// new table in this transaction.
				if (!droppedTables.Contains(tableId)) {
					var source = TableComposite.GetTableSource(tableId);
					if (!normalizedChangedTables.Any(x => x.TableSource.TableId.Equals(source.TableId))) {
						// This is for entries that are created but modified (no registries).
						normalizedChangedTables.Add(new CommitTableInfo(source));
					}
				}
			}

			return normalizedChangedTables.ToArray();
		}

		private IEnumerable<TableSource> GetNormalizedDroppedTables(IEnumerable<int> created, IEnumerable<int> dropped) {
			// Create a normalized list of MasterTableDataSource of all tables that
			// were dropped (and not created) in this transaction.  This list
			// represents tables that will be dropped if the transaction
			// successfully commits.

			var normalizedDroppedTables = new List<TableSource>(8);

			var createdTables = created.ToList();

			foreach (var tableId in dropped) {
				// Was this dropped table also created?  If it was created in this
				// transaction then we don't care about it.
				if (!createdTables.Contains(tableId)) {
					var masterTable = TableComposite.GetTableSource(tableId);
					normalizedDroppedTables.Add(masterTable);
				}
			}

			return normalizedDroppedTables.ToArray();
		}

		#region CommitTableInfo

		private sealed class CommitTableInfo {
			public CommitTableInfo(TableSource tableSource) 
				: this(tableSource, null, null) {
			}

			public CommitTableInfo(TableSource tableSource, TableEventRegistry eventRegistry, IEnumerable<TableEventRegistry> changes) {
				TableSource = tableSource;
				EventRegistry = eventRegistry;
				ChangesSinceCommit = changes;
			}

			public TableSource TableSource { get; private set; }

			public IIndexSet IndexSet { get; set; }

			public TableEventRegistry EventRegistry { get; private set; }

			public IEnumerable<TableEventRegistry> ChangesSinceCommit { get; private set; }

			public bool HasChangesSinceCommit {
				get { return ChangesSinceCommit != null && ChangesSinceCommit.Any(); }
			}

			public int[] AddedRows { get; set; }

			public int[] RemovedRows { get; set; }
		}

		#endregion

		#region ConflictResolveInfo

		class ConflictResolveInfo {
			public ConflictResolveInfo(int commitId, IList<TransactionObjectState> objectStates) {
				CommitId = commitId;
				ObjectStates = objectStates;
			}

			public int CommitId { get; private set; }

			public IList<TransactionObjectState> ObjectStates { get; private set; }

			public int[] SelectedTables { get; set; }

			public IList<ObjectName> ObjectsDropped { get; set; }

			public IEnumerable<TableEventRegistry> ChangedTables { get; set; }

			public IList<ObjectName> ObjectsCreated { get; set; }

			public IList<int> CreatedTables { get; set; }

			public IList<int> DroppedTables { get; set; }
		}

		#endregion
	}
}
