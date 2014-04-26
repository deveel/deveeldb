// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Index;
using Deveel.Diagnostics;

namespace Deveel.Data.Transactions {
	public sealed class TransactionWork {
		internal TableDataConglomerate Conglomerate { get; private set; }

		internal Transaction Transaction { get; private set; }

		public IEnumerable<MasterTableDataSource> SelectedFromTables { get; private set; }

		internal TransactionWork(TableDataConglomerate conglomerate, Transaction transaction, IEnumerable<MasterTableDataSource> selectedFromTables, IEnumerable<IMutableTableDataSource> touchedTables, TransactionJournal journal) {
			Conglomerate = conglomerate;
			Transaction = transaction;
			SelectedFromTables = selectedFromTables;

			// Get individual journals for updates made to tables in this
			// transaction.
			// The list MasterTableJournal

			ChangedTables = touchedTables.Select(t => t.Journal).Where(tableJournal => tableJournal.EntriesCount > 0);

			// The list of tables created by this journal.
			CreatedTables = journal.GetTablesCreated();
			// Ths list of tables dropped by this journal.
			DroppedTables = journal.GetTablesDropped();
			// The list of tables that constraints were alter by this journal
			ConstraintAlteredTables = journal.GetTablesConstraintAltered();

			// Get the list of all database objects that were created in the
			// transaction.
			ObjectsCreated = transaction.AllNamesCreated;
			// Get the list of all database objects that were dropped in the
			// transaction.
			ObjectsDropped = transaction.AllNamesDropped;

			CommitId = transaction.CommitId;
		}

		public IEnumerable<int> CreatedTables { get; private set; }

		public IEnumerable<int> DroppedTables { get; private set; }

		public IEnumerable<int> ConstraintAlteredTables { get; private set; }

		public IEnumerable<MasterTableJournal> ChangedTables { get; private set; }

		public IEnumerable<TableName> ObjectsCreated { get; private set; }

		public IEnumerable<TableName> ObjectsDropped { get; private set; }

		public bool Done { get; private set; }

		public long CommitId { get; private set; }

		public bool HasChanges {
			get { return CreatedTables.Any() || DroppedTables.Any() || ConstraintAlteredTables.Any() || ChangedTables.Any(); }
		}

		/// <summary>
		/// Returns true if the given List of <see cref="CommitTableInfo"/> objects 
		/// contains an entry for the given master table.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="master"></param>
		/// <returns></returns>
		private static bool CommitTableListContains(IEnumerable<CommitTableInfo> list, MasterTableDataSource master) {
			return list.Any(info => info.Master.Equals(master));
		}

		private void AssertNoDirtySelect() {
			// We only perform this check if transaction error on dirty selects
			// are enabled.
			if (Transaction.TransactionErrorOnDirtySelect) {
				// For each table that this transaction selected from, if there are
				// any committed changes then generate a transaction error.
				foreach (MasterTableDataSource selectedTable in SelectedFromTables) {
					// Find all committed journals equal to or greater than this
					// transaction's commit_id.
					MasterTableJournal[] journalsSince = selectedTable.FindAllJournalsSince(CommitId);
					if (journalsSince.Length > 0) {
						// Yes, there are changes so generate transaction error and
						// rollback.
						throw new TransactionException(
							TransactionException.DirtyTableSelect,
							"Concurrent Serializable Transaction Conflict(4): " +
							"Select from table that has committed changes: " +
							selectedTable.Name);
					}
				}
			}
		}

		internal void CheckConflicts(IEnumerable<NameSpaceJournal> namespaceJournals) {
			AssertNoDirtySelect();

			// Check there isn't a namespace clash with database objects.
			// We need to create a list of all create and drop activity in the
			// conglomerate from when the transaction started.
			var allDroppedObs = new List<TableName>();
			var allCreatedObs = new List<TableName>();
			foreach (NameSpaceJournal nsJournal in namespaceJournals) {
				if (nsJournal.CommitId >= CommitId) {
					allDroppedObs.AddRange(nsJournal.DroppedNames);
					allCreatedObs.AddRange(nsJournal.CreatedNames);
				}
			}

			// The list of all dropped objects since this transaction
			// began.
			bool conflict5 = false;
			object conflictName = null;
			string conflictDesc = "";
			foreach (TableName droppedOb in allDroppedObs) {
				if (ObjectsDropped.Contains(droppedOb)) {
					conflict5 = true;
					conflictName = droppedOb;
					conflictDesc = "Drop Clash";
				}
			}
			// The list of all created objects since this transaction
			// began.
			foreach (TableName createdOb in allCreatedObs) {
				if (ObjectsCreated.Contains(createdOb)) {
					conflict5 = true;
					conflictName = createdOb;
					conflictDesc = "Create Clash";
				}
			}
			if (conflict5) {
				// Namespace conflict...
				throw new TransactionException(
					TransactionException.DuplicateTable,
					"Concurrent Serializable Transaction Conflict(5): " +
					"Namespace conflict: " + conflictName + " " +
					conflictDesc);
			}

			// For each journal,
			foreach (MasterTableJournal changeJournal in ChangedTables) {
				// The table the change was made to.
				int tableId = changeJournal.TableId;
				// Get the master table with this table id.
				MasterTableDataSource master = Conglomerate.GetMasterTable(tableId);

				// True if the state contains a committed resource with the given name
				bool committedResource = Conglomerate.ContainsVisibleResource(tableId);

				// Check this table is still in the committed tables list.
				if (!CreatedTables.Contains(tableId) && !committedResource) {
					// This table is no longer a committed table, so rollback
					throw new TransactionException(
						TransactionException.TableDropped,
						"Concurrent Serializable Transaction Conflict(2): " +
						"Table altered/dropped: " + master.Name);
				}

				// Since this journal was created, check to see if any changes to the
				// tables have been committed since.
				// This will return all journals on the table with the same commit_id
				// or greater.
				MasterTableJournal[] journalsSince = master.FindAllJournalsSince(CommitId);

				// For each journal, determine if there's any clashes.
				foreach (MasterTableJournal tableJournal in journalsSince) {
					// This will thrown an exception if a commit classes.
					changeJournal.TestCommitClash(master.TableInfo, tableJournal);
				}
			}

			// Look at the transaction journal, if a table is dropped that has
			// journal entries since the last commit then we have an exception
			// case.
			foreach (int tableId in DroppedTables) {
				// Get the master table with this table id.
				MasterTableDataSource master = Conglomerate.GetMasterTable(tableId);
				// Any journal entries made to this dropped table?
				if (master.FindAllJournalsSince(CommitId).Length > 0) {
					// Oops, yes, rollback!
					throw new TransactionException(
						TransactionException.TableRemoveClash,
						"Concurrent Serializable Transaction Conflict(3): " +
						"Dropped table has modifications: " + master.Name);
				}
			}
		}

		private CommitTableInfo[] GetNormalizedChangedTables() {
			// Create a normalized list of MasterTableDataSource of all tables that
			// were either changed (and not dropped), and created (and not dropped).
			// This list represents all tables that are either new or changed in
			// this transaction.

			var normalizedChangedTables = new List<CommitTableInfo>(8);

			// Add all tables that were changed and not dropped in this transaction.

			normalizedChangedTables.AddRange(
				ChangedTables.Select(tableJournal => new { tableJournal, tableId = tableJournal.TableId })
					.Where(t => !DroppedTables.Contains(t.tableId))
					.Select(t => new { t, masterTable = Conglomerate.GetMasterTable(t.tableId) })
					.Select(t => new CommitTableInfo {
						Master = t.masterTable,
						Journal = t.t.tableJournal,
						ChangesSinceCommit = t.masterTable.FindAllJournalsSince(CommitId)
					}));

			// Add all tables that were created and not dropped in this transaction.
			foreach (var tableId in CreatedTables) {
				// If this table is not dropped in this transaction then this is a
				// new table in this transaction.
				if (!DroppedTables.Contains(tableId)) {
					MasterTableDataSource masterTable = Conglomerate.GetMasterTable(tableId);
					if (!CommitTableListContains(normalizedChangedTables, masterTable)) {

						// This is for entries that are created but modified (no journal).
						var tableInfo = new CommitTableInfo {
							Master = masterTable
						};

						normalizedChangedTables.Add(tableInfo);
					}
				}
			}

			return normalizedChangedTables.ToArray();
		}

		private MasterTableDataSource[] GetNormalizedDroppedTables() {
			// Create a normalized list of MasterTableDataSource of all tables that
			// were dropped (and not created) in this transaction.  This list
			// represents tables that will be dropped if the transaction
			// successfully commits.

			var normalizedDroppedTables = new List<MasterTableDataSource>(8);
			foreach (var tableId in DroppedTables) {
				// Was this dropped table also created?  If it was created in this
				// transaction then we don't care about it.
				if (!CreatedTables.Contains(tableId)) {
					MasterTableDataSource masterTable = Conglomerate.GetMasterTable(tableId);
					normalizedDroppedTables.Add(masterTable);
				}
			}

			return normalizedDroppedTables.ToArray();
		}

		private ITableDataSource[] FindChangedTables(Transaction checkTransaction, CommitTableInfo[] normalizedChangedTables) {
			var changedTableSource = new ITableDataSource[normalizedChangedTables.Length];

			// Set up the above arrays
			for (int i = 0; i < normalizedChangedTables.Length; ++i) {
				// Get the information for this changed table
				CommitTableInfo tableInfo = normalizedChangedTables[i];

				// Get the master table that changed from the normalized list.
				MasterTableDataSource master = tableInfo.Master;
				// Did this table change since the transaction started?
				MasterTableJournal[] allTableChanges = tableInfo.ChangesSinceCommit;

				if (allTableChanges == null || allTableChanges.Length == 0) {
					// No changes so we can pick the correct IIndexSet from the current
					// transaction.

					// Get the state of the changed tables from the Transaction
					IMutableTableDataSource mtable = Transaction.GetMutableTable(master.TableName);
					// Get the current index set of the changed table
					tableInfo.IndexSet = Transaction.GetIndexSetForTable(master);
					// Flush all index changes in the table
					mtable.FlushIndexChanges();

					// Set the 'check_transaction' object with the latest version of the
					// table.
					checkTransaction.UpdateVisibleTable(tableInfo.Master, tableInfo.IndexSet);
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
					IMutableTableDataSource mtable = master.CreateTableDataSourceAtCommit(checkTransaction, tableInfo.Journal);
					// Get the current index set of the changed table
					tableInfo.IndexSet = checkTransaction.GetIndexSetForTable(master);
					// Flush all index changes in the table
					mtable.FlushIndexChanges();

					// Dispose the table
					mtable.Dispose();
				}

				// And now refresh the 'changedTableSource' entry
				changedTableSource[i] = checkTransaction.GetTable(master.TableName);
			}

			return changedTableSource;
		}

		private void FireChangeEvents(Transaction checkTransaction, CommitTableInfo[] normalizedChangedTables, EventHandlerList modificationEvents) {
			// For each changed table.
			//n_loop:
			for (int i = 0; i < normalizedChangedTables.Length; ++i) {
				CommitTableInfo tableInfo = normalizedChangedTables[i];
				// Get the journal that details the change to the table.
				MasterTableJournal changeJournal = tableInfo.Journal;
				if (changeJournal != null) {
					// Get the table name
					TableName tableName = tableInfo.Master.TableName;
					// The list of listeners to dispatch this event to
					CommitModificationEventHandler modificationHandler;

					// Are there any listeners listening for events on this table?
					lock (modificationEvents) {
						modificationHandler = modificationEvents[tableName] as CommitModificationEventHandler;
						if (modificationHandler == null) {
							// If no listeners on this table, continue to the next
							// table that was changed.
							continue;
						}
					}

					// Generate the event
					var args = new CommitModificationEventArgs(tableName, tableInfo.NormalizedAddedRows, tableInfo.NormalizedRemovedRows);
					// Fire this event on the listeners
					modificationHandler(checkTransaction, args);
				} // if (changeJournal != null)
			} // for each changed table
		}

		private void CheckConstraintViolations(Transaction checkTransaction, CommitTableInfo[] normalizedChangedTables, ITableDataSource[] changedTableSource) {
			// Any tables that the constraints were altered for we need to check
			// if any rows in the table violate the new constraints.
			foreach (var tableId in ConstraintAlteredTables) {
				// We need to check there are no constraint violations for all the
				// rows in the table.
				for (int n = 0; n < normalizedChangedTables.Length; ++n) {
					CommitTableInfo tableInfo = normalizedChangedTables[n];
					if (tableInfo.Master.TableId == tableId) {
						TableDataConglomerate.CheckAllAddConstraintViolations(checkTransaction, changedTableSource[n], ConstraintDeferrability.InitiallyDeferred);
					}
				}
			}

			// For each changed table we must determine the rows that
			// were deleted and perform the remove constraint checks on the
			// deleted rows.  Note that this happens after the records are
			// removed from the index.

			// For each changed table,
			for (int i = 0; i < normalizedChangedTables.Length; ++i) {
				CommitTableInfo tableInfo = normalizedChangedTables[i];
				// Get the journal that details the change to the table.
				MasterTableJournal changeJournal = tableInfo.Journal;
				if (changeJournal != null) {
					// Find the normalized deleted rows.
					int[] normalizedRemovedRows = changeJournal.NormalizedRemovedRows();
					// Check removing any of the data doesn't cause a constraint
					// violation.
					TableDataConglomerate.CheckRemoveConstraintViolations(checkTransaction, changedTableSource[i], normalizedRemovedRows, ConstraintDeferrability.InitiallyDeferred);

					// Find the normalized added rows.
					int[] normalizedAddedRows = changeJournal.NormalizedAddedRows();
					// Check adding any of the data doesn't cause a constraint
					// violation.
					TableDataConglomerate.CheckAddConstraintViolations(checkTransaction, changedTableSource[i], normalizedAddedRows, ConstraintDeferrability.InitiallyDeferred);

					// Set up the list of added and removed rows
					tableInfo.NormalizedAddedRows = normalizedAddedRows;
					tableInfo.NormalizedRemovedRows = normalizedRemovedRows;

				}
			}
		}

		internal void Commit(IList<NameSpaceJournal> nameSpaceJournals, EventHandlerList modificationEvents) {
			var changedTablesList = new List<MasterTableDataSource>();

			// This is a transaction that will represent the view of the database
			// at the end of the commit
			Transaction checkTransaction = null;

			bool entriesCommitted = false;

			try {
				// ---- Commit check stage ----
				CheckConflicts(nameSpaceJournals);

				// Tests passed so go on to commit,

				// ---- Commit stage ----

				var normalizedChangedTables = GetNormalizedChangedTables();
				var normalizedDroppedTables = GetNormalizedDroppedTables();

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
				checkTransaction = Conglomerate.CreateTransaction();

				// Overwrite this view with tables from this transaction that have
				// changed or have been added or dropped.

				// (Note that order here is important).  First drop any tables from
				// this view.
				foreach (MasterTableDataSource masterTable in normalizedDroppedTables) {
					// Drop this table in the current view
					checkTransaction.RemoveVisibleTable(masterTable);
				}

				// Now add any changed tables to the view.

				// Represents view of the changed tables
				var changedTableSource = FindChangedTables(checkTransaction, normalizedChangedTables);

				// The 'checkTransaction' now represents the view the database will be
				// if the commit succeeds.  We Lock 'checkTransaction' so it is
				// Read-only (the view is immutable).
				checkTransaction.SetReadOnly();

				CheckConstraintViolations(checkTransaction, normalizedChangedTables, changedTableSource);

				// Deferred trigger events.
				FireChangeEvents(checkTransaction, normalizedChangedTables, modificationEvents);

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
				foreach (CommitTableInfo tableInfo in normalizedChangedTables) {
					// Get the journal that details the change to the table.
					MasterTableJournal changeJournal = tableInfo.Journal;
					if (changeJournal != null) {
						// Get the master table with this table id.
						MasterTableDataSource master = tableInfo.Master;
						// Commit the changes to the table.
						// We use 'this.commit_id' which is the current commit level we are
						// at.
						master.CommitTransactionChange(Conglomerate.CommitId, changeJournal, tableInfo.IndexSet);
						// Add to 'changed_tables_list'
						changedTablesList.Add(master);
					}
				}

				// Only do this if we've created or dropped tables.
				if (CreatedTables.Any() || DroppedTables.Any()) {
					// Update the committed tables in the conglomerate state.
					// This will update and synchronize the headers in this conglomerate.
					Conglomerate.CommitToTables(CreatedTables, DroppedTables);
				}

				// Update the namespace clash list
				if (ObjectsCreated.Any() || ObjectsDropped.Any()) {
					nameSpaceJournals.Add(new NameSpaceJournal(CommitId, ObjectsCreated, ObjectsDropped));
				}
			} finally {
				try {
					// If entries_committed == false it means we didn't get to a point
					// where any changed tables were committed.  Attempt to rollback the
					// changes in this transaction if they haven't been committed yet.
					if (entriesCommitted == false) {
						// For each change to each table,
						foreach (MasterTableJournal changeJournal in ChangedTables) {
							// The table the changes were made to.
							int tableId = changeJournal.TableId;
							// Get the master table with this table id.
							MasterTableDataSource master = Conglomerate.GetMasterTable(tableId);
							// Commit the rollback on the table.
							master.RollbackTransactionChange(changeJournal);
						}

						if (Conglomerate.Logger.IsInterestedIn(LogLevel.Information))
							Conglomerate.Logger.Info(this, "Rolled back transaction changes in a commit.");
					}
				} finally {
					try {
						// Dispose the 'checkTransaction'
						if (checkTransaction != null) {
							checkTransaction.CloseAndCleanup();
							Conglomerate.CloseTransaction(checkTransaction);
						}
						// Always ensure a transaction close, even if we have an exception.
						// Notify the conglomerate that this transaction has closed.
						Conglomerate.CloseTransaction(Transaction);
					} catch (Exception e) {
						Conglomerate.Logger.Error(this, e);
					} finally {
						Done = true;
					}
				}
			}
		}

		/// <summary>
		/// A static container class for information collected about a table 
		/// during the commit cycle.
		/// </summary>
		private sealed class CommitTableInfo {
			// The master table
			public MasterTableDataSource Master;
			// The immutable index set
			public IIndexSet IndexSet;
			// The journal describing the changes to this table by this
			// transaction.
			public MasterTableJournal Journal;
			// A list of journals describing changes since this transaction
			// started.
			public MasterTableJournal[] ChangesSinceCommit;
			// Break down of changes to the table
			// Normalized list of row ids that were added
			public int[] NormalizedAddedRows;
			// Normalized list of row ids that were removed
			public int[] NormalizedRemovedRows;
		}
	}
}