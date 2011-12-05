// 
//  Copyright 2010-2011  Deveel
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
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;

using Deveel.Data.Collections;
using Deveel.Diagnostics;

namespace Deveel.Data {
	public sealed partial class TableDataConglomerate {
		/// <summary>
		/// The list of all name space journals for the history of committed 
		/// transactions.
		/// </summary>
		private readonly List<NameSpaceJournal> namespaceJournalList;

		// ---------- Table event listener ----------

		/// <summary>
		/// All listeners for modification events on tables in this conglomerate.
		/// </summary>
		private readonly EventHandlerList modificationEvents;


		/// <summary>
		/// Tries to commit a transaction to the conglomerate.
		/// </summary>
		/// <param name="transaction">The transaction to commit from.</param>
		/// <param name="visible_tables">The list of visible tables at the end 
		/// of the commit (<see cref="MasterTableDataSource"/>)</param>
		/// <param name="selectedFromTables">The list of tables that this 
		/// transaction performed <i>select</i> like queries on (<see cref="MasterTableDataSource"/>)</param>
		/// <param name="touchedTables">The list of tables touched by the 
		/// transaction (<see cref="IMutableTableDataSource"/>)</param>
		/// <param name="journal">The journal that describes all the changes 
		/// within the transaction.</param>
		/// <remarks>
		/// This is called by the <see cref="Transaction.Commit"/> 
		/// method in <see cref="Transaction"/>. An overview of how this works 
		/// follows:
		/// <list type="bullet">
		///   <item>Determine if any transactions have been committed since 
		///   this transaction was created.</item>
		///   <item>If no transactions committed then commit this transaction 
		///   and exit.</item>
		///   <item>Otherwise, determine the tables that have been changed by 
		///   the committed transactions since this was created.</item>
		///   <item>If no tables changed in the tables changed by this transaction 
		///   then commit this transaction and exit.</item>
		///   <item>Determine if there are any rows that have been deleted that 
		///   this transaction read/deleted.</item>
		///   <item>If there are then rollback this transaction and throw an error.</item>
		///   <item>Determine if any rows have been added to the tables this 
		///   transaction read/changed.</item>
		///   <item>If there are then rollback this transaction and throw an error.</item>
		///   <item>Otherwise commit the transaction.</item>
		/// </list>
		/// </remarks>
		internal void ProcessCommit(Transaction transaction, ArrayList visible_tables,
						   IEnumerable<MasterTableDataSource> selectedFromTables,
						   IList<IMutableTableDataSource> touchedTables, TransactionJournal journal) {

			// Get individual journals for updates made to tables in this
			// transaction.
			// The list MasterTableJournal
			List<MasterTableJournal> journalList = new List<MasterTableJournal>();
			for (int i = 0; i < touchedTables.Count; ++i) {
				MasterTableJournal tableJournal = touchedTables[i].Journal;
				if (tableJournal.EntriesCount > 0) {
					// Check the journal has entries.
					journalList.Add(tableJournal);
				}
			}

			MasterTableJournal[] changedTables = journalList.ToArray();

			// The list of tables created by this journal.
			IntegerVector createdTables = journal.GetTablesCreated();
			// Ths list of tables dropped by this journal.
			IntegerVector droppedTables = journal.GetTablesDropped();
			// The list of tables that constraints were alter by this journal
			IntegerVector constraintAlteredTables = journal.GetTablesConstraintAltered();

			// Exit early if nothing changed (this is a Read-only transaction)
			if (changedTables.Length == 0 &&
				createdTables.Count == 0 && droppedTables.Count == 0 &&
				constraintAlteredTables.Count == 0) {
				CloseTransaction(transaction);
				return;
			}

			// This flag is set to true when entries from the changes tables are
			// at a point of no return.  If this is false it is safe to rollback
			// changes if necessary.
			bool entriesCommitted = false;

			// The tables that were actually changed (MasterTableDataSource)
			List<MasterTableDataSource> changedTablesList = new List<MasterTableDataSource>();

			// Grab the commit Lock.
			lock (CommitLock) {
				// Get the list of all database objects that were created in the
				// transaction.
				IList<TableName> databaseObjectsCreated = transaction.AllNamesCreated;
				// Get the list of all database objects that were dropped in the
				// transaction.
				IList<TableName> databaseObjectsDropped = transaction.AllNamesDropped;

				// This is a transaction that will represent the view of the database
				// at the end of the commit
				Transaction checkTransaction = null;

				try {
					// ---- Commit check stage ----

					long tranCommitId = transaction.CommitID;

					// We only perform this check if transaction error on dirty selects
					// are enabled.
					if (transaction.TransactionErrorOnDirtySelect) {
						// For each table that this transaction selected from, if there are
						// any committed changes then generate a transaction error.
						foreach (MasterTableDataSource selectedTable in selectedFromTables) {
							// Find all committed journals equal to or greater than this
							// transaction's commit_id.
							MasterTableJournal[] journalsSince = selectedTable.FindAllJournalsSince(tranCommitId);
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

					// Check there isn't a namespace clash with database objects.
					// We need to create a list of all create and drop activity in the
					// conglomerate from when the transaction started.
					List<TableName> allDroppedObs = new List<TableName>();
					List<TableName> allCreatedObs = new List<TableName>();
					foreach (NameSpaceJournal nsJournal in namespaceJournalList) {
						if (nsJournal.CommitId >= tranCommitId) {
							allDroppedObs.AddRange(nsJournal.DroppedNames);
							allCreatedObs.AddRange(nsJournal.CreatedNames);
						}
					}

					// The list of all dropped objects since this transaction
					// began.
					bool conflict5 = false;
					object conflict_name = null;
					string conflict_desc = "";
					foreach (TableName droppedOb in allDroppedObs) {
						if (databaseObjectsDropped.Contains(droppedOb)) {
							conflict5 = true;
							conflict_name = droppedOb;
							conflict_desc = "Drop Clash";
						}
					}
					// The list of all created objects since this transaction
					// began.
					foreach (TableName createdOb in allCreatedObs) {
						if (databaseObjectsCreated.Contains(createdOb)) {
							conflict5 = true;
							conflict_name = createdOb;
							conflict_desc = "Create Clash";
						}
					}
					if (conflict5) {
						// Namespace conflict...
						throw new TransactionException(
							TransactionException.DuplicateTable,
							"Concurrent Serializable Transaction Conflict(5): " +
							"Namespace conflict: " + conflict_name + " " +
							conflict_desc);
					}

					// For each journal,
					foreach (MasterTableJournal changeJournal in changedTables) {
						// The table the change was made to.
						int tableId = changeJournal.TableId;
						// Get the master table with this table id.
						MasterTableDataSource master = GetMasterTable(tableId);

						// True if the state contains a committed resource with the given name
						bool committedResource = stateStore.ContainsVisibleResource(tableId);

						// Check this table is still in the committed tables list.
						if (!createdTables.Contains(tableId) && !committedResource) {
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
						MasterTableJournal[] journalsSince = master.FindAllJournalsSince(tranCommitId);

						// For each journal, determine if there's any clashes.
						foreach (MasterTableJournal tableJournal in journalsSince) {
							// This will thrown an exception if a commit classes.
							changeJournal.TestCommitClash(master.DataTableInfo, tableJournal);
						}
					}

					// Look at the transaction journal, if a table is dropped that has
					// journal entries since the last commit then we have an exception
					// case.
					for (int i = 0; i < droppedTables.Count; ++i) {
						int tableId = droppedTables[i];
						// Get the master table with this table id.
						MasterTableDataSource master = GetMasterTable(tableId);
						// Any journal entries made to this dropped table?
						if (master.FindAllJournalsSince(tranCommitId).Length > 0) {
							// Oops, yes, rollback!
							throw new TransactionException(
								TransactionException.TableRemoveClash,
								"Concurrent Serializable Transaction Conflict(3): " +
								"Dropped table has modifications: " + master.Name);
						}
					}

					// Tests passed so go on to commit,

					// ---- Commit stage ----

					// Create a normalized list of MasterTableDataSource of all tables that
					// were either changed (and not dropped), and created (and not dropped).
					// This list represents all tables that are either new or changed in
					// this transaction.

					List<CommitTableInfo> normalizedChangedTables = new List<CommitTableInfo>(8);
					// Add all tables that were changed and not dropped in this transaction.
					foreach (MasterTableJournal tableJournal in changedTables) {
						// The table the changes were made to.
						int tableId = tableJournal.TableId;
						// If this table is not dropped in this transaction and is not
						// already in the normalized list then add it.
						if (!droppedTables.Contains(tableId)) {
							MasterTableDataSource masterTable = GetMasterTable(tableId);

							CommitTableInfo tableInfo = new CommitTableInfo();
							tableInfo.Master = masterTable;
							tableInfo.Journal = tableJournal;
							tableInfo.ChangesSinceCommit = masterTable.FindAllJournalsSince(tranCommitId);

							normalizedChangedTables.Add(tableInfo);
						}
					}

					int createdTablesCount = createdTables.Count;
					// Add all tables that were created and not dropped in this transaction.
					for (int i = 0; i < createdTablesCount; ++i) {
						int tableId = createdTables[i];
						// If this table is not dropped in this transaction then this is a
						// new table in this transaction.
						if (!droppedTables.Contains(tableId)) {
							MasterTableDataSource masterTable = GetMasterTable(tableId);
							if (!CommitTableListContains(normalizedChangedTables, masterTable)) {

								// This is for entries that are created but modified (no journal).
								CommitTableInfo tableInfo = new CommitTableInfo();
								tableInfo.Master = masterTable;

								normalizedChangedTables.Add(tableInfo);
							}
						}
					}

					// The final size of the normalized changed tables list
					int normChangedTablesCount = normalizedChangedTables.Count;

					// Create a normalized list of MasterTableDataSource of all tables that
					// were dropped (and not created) in this transaction.  This list
					// represents tables that will be dropped if the transaction
					// successfully commits.

					int droppedTablesCount = droppedTables.Count;
					List<MasterTableDataSource> normalizedDroppedTables = new List<MasterTableDataSource>(8);
					for (int i = 0; i < droppedTablesCount; ++i) {
						// The dropped table
						int tableId = droppedTables[i];
						// Was this dropped table also created?  If it was created in this
						// transaction then we don't care about it.
						if (!createdTables.Contains(tableId)) {
							MasterTableDataSource masterTable = GetMasterTable(tableId);
							normalizedDroppedTables.Add(masterTable);
						}
					}

					// We now need to create a SimpleTransaction object that we
					// use to send to the triggering mechanism.  This
					// SimpleTransaction represents a very specific view of the
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
					checkTransaction = CreateTransaction();

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
					ITableDataSource[] changedTableSource = new ITableDataSource[normChangedTablesCount];
					// Set up the above arrays
					for (int i = 0; i < normChangedTablesCount; ++i) {
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
							IMutableTableDataSource mtable = transaction.GetTable(master.TableName);
							// Get the current index set of the changed table
							tableInfo.IndexSet = transaction.GetIndexSetForTable(master);
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

					// The 'checkTransaction' now represents the view the database will be
					// if the commit succeeds.  We Lock 'checkTransaction' so it is
					// Read-only (the view is immutable).
					checkTransaction.SetReadOnly();

					// Any tables that the constraints were altered for we need to check
					// if any rows in the table violate the new constraints.
					for (int i = 0; i < constraintAlteredTables.Count; ++i) {
						// We need to check there are no constraint violations for all the
						// rows in the table.
						int tableId = constraintAlteredTables[i];
						for (int n = 0; n < normChangedTablesCount; ++n) {
							CommitTableInfo tableInfo = normalizedChangedTables[n];
							if (tableInfo.Master.TableID == tableId) {
								CheckAllAddConstraintViolations(checkTransaction, changedTableSource[n], ConstraintDeferrability.InitiallyDeferred);
							}
						}
					}

					// For each changed table we must determine the rows that
					// were deleted and perform the remove constraint checks on the
					// deleted rows.  Note that this happens after the records are
					// removed from the index.

					// For each changed table,
					for (int i = 0; i < normChangedTablesCount; ++i) {
						CommitTableInfo tableInfo = normalizedChangedTables[i];
						// Get the journal that details the change to the table.
						MasterTableJournal changeJournal = tableInfo.Journal;
						if (changeJournal != null) {
							// Find the normalized deleted rows.
							int[] normalizedRemovedRows = changeJournal.NormalizedRemovedRows();
							// Check removing any of the data doesn't cause a constraint
							// violation.
							CheckRemoveConstraintViolations(checkTransaction, changedTableSource[i], normalizedRemovedRows, ConstraintDeferrability.InitiallyDeferred);

							// Find the normalized added rows.
							int[] normalizedAddedRows = changeJournal.NormalizedAddedRows();
							// Check adding any of the data doesn't cause a constraint
							// violation.
							CheckAddConstraintViolations(checkTransaction, changedTableSource[i], normalizedAddedRows, ConstraintDeferrability.InitiallyDeferred);

							// Set up the list of added and removed rows
							tableInfo.NormalizedAddedRows = normalizedAddedRows;
							tableInfo.NormalizedRemovedRows = normalizedRemovedRows;

						}
					}

					// Deferred trigger events.
					// For each changed table.
					//n_loop:
					for (int i = 0; i < normChangedTablesCount; ++i) {
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
							CommitModificationEventArgs args = new CommitModificationEventArgs(tableName, tableInfo.NormalizedAddedRows,
							                                                                   tableInfo.NormalizedRemovedRows);
							// Fire this event on the listeners
							modificationHandler(checkTransaction, args);
						} // if (changeJournal != null)
					} // for each changed table

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
							master.CommitTransactionChange(commitId, changeJournal, tableInfo.IndexSet);
							// Add to 'changed_tables_list'
							changedTablesList.Add(master);
						}
					}

					// Only do this if we've created or dropped tables.
					if (createdTables.Count > 0 || droppedTables.Count > 0) {
						// Update the committed tables in the conglomerate state.
						// This will update and synchronize the headers in this conglomerate.
						CommitToTables(createdTables, droppedTables);
					}

					// Update the namespace clash list
					if (databaseObjectsCreated.Count > 0 ||
						databaseObjectsDropped.Count > 0) {
						namespaceJournalList.Add(new NameSpaceJournal(tranCommitId, databaseObjectsCreated, databaseObjectsDropped));
					}
				} finally {
					try {
						// If entries_committed == false it means we didn't get to a point
						// where any changed tables were committed.  Attempt to rollback the
						// changes in this transaction if they haven't been committed yet.
						if (entriesCommitted == false) {
							// For each change to each table,
							foreach (MasterTableJournal changeJournal in changedTables) {
								// The table the changes were made to.
								int table_id = changeJournal.TableId;
								// Get the master table with this table id.
								MasterTableDataSource master = GetMasterTable(table_id);
								// Commit the rollback on the table.
								master.RollbackTransactionChange(changeJournal);
							}

							if (Debug.IsInterestedIn(DebugLevel.Information))
								Debug.Write(DebugLevel.Information, this, "Rolled back transaction changes in a commit.");
						}
					} finally {
						try {
							// Dispose the 'check_transaction'
							if (checkTransaction != null) {
								checkTransaction.dispose();
								CloseTransaction(checkTransaction);
							}
							// Always ensure a transaction close, even if we have an exception.
							// Notify the conglomerate that this transaction has closed.
							CloseTransaction(transaction);
						} catch (Exception e) {
							Debug.WriteException(e);
						}
					}
				}

				// Flush the journals up to the minimum commit id for all the tables
				// that this transaction changed.
				long minCommitId = openTransactions.MinimumCommitID(null);
				foreach (MasterTableDataSource master in changedTablesList) {
					master.MergeJournalChanges(minCommitId);
				}
				int nsjsz = namespaceJournalList.Count;
				for (int i = nsjsz - 1; i >= 0; --i) {
					NameSpaceJournal namespaceJournal = namespaceJournalList[i];
					// Remove if the commit id for the journal is less than the minimum
					// commit id
					if (namespaceJournal.CommitId < minCommitId) {
						namespaceJournalList.RemoveAt(i);
					}
				}

				// Set a check point in the store system.  This means that the
				// persistance state is now stable.
				storeSystem.SetCheckPoint();

			} // lock (commit_lock)
		}

		/// <summary>
		/// Rollbacks a transaction and invalidates any changes that the 
		/// transaction made to the database.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="touchedTables"></param>
		/// <param name="journal"></param>
		/// <remarks>
		/// The rows that this transaction changed are given up as freely 
		/// available rows. This is called by the <see cref="Transaction.Rollback"/> 
		/// method in <see cref="Transaction"/>.
		/// </remarks>
		internal void ProcessRollback(Transaction transaction, IList<IMutableTableDataSource> touchedTables, TransactionJournal journal) {
			// Go through the journal.  Any rows added should be marked as deleted
			// in the respective master table.

			// Get individual journals for updates made to tables in this
			// transaction.
			// The list MasterTableJournal
			List<MasterTableJournal> journalList = new List<MasterTableJournal>();
			for (int i = 0; i < touchedTables.Count; ++i) {
				MasterTableJournal tableJournal = touchedTables[i].Journal;
				if (tableJournal.EntriesCount > 0) // Check the journal has entries.
					journalList.Add(tableJournal);
			}

			MasterTableJournal[] changedTables = journalList.ToArray();

			lock (CommitLock) {
				try {
					// For each change to each table,
					foreach (MasterTableJournal changeJournal in changedTables) {
						// The table the changes were made to.
						int tableId = changeJournal.TableId;
						// Get the master table with this table id.
						MasterTableDataSource master = GetMasterTable(tableId);
						// Commit the rollback on the table.
						master.RollbackTransactionChange(changeJournal);
					}
				} finally {
					// Notify the conglomerate that this transaction has closed.
					CloseTransaction(transaction);
				}
			}
		}

		/// <summary>
		/// Returns true if the given List of <see cref="CommitTableInfo"/> objects 
		/// contains an entry for the given master table.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="master"></param>
		/// <returns></returns>
		private static bool CommitTableListContains(IList<CommitTableInfo> list, MasterTableDataSource master) {
			foreach (CommitTableInfo info in list) {
				if (info.Master.Equals(master))
					return true;
			}
			return false;
		}

		/// <summary>
		/// Sets the <see cref="MasterTableDataSource"/> objects pointed by the
		/// given <see cref="IntegerVector"/> to the currently committed list of 
		/// tables in this conglomerate.
		/// </summary>
		/// <param name="createdTables"></param>
		/// <param name="droppedTables"></param>
		/// <remarks>
		/// This will make the change permanent by updating the state file also.
		/// <para>
		/// This should be called as part of a transaction commit.
		/// </para>
		/// </remarks>
		private void CommitToTables(IntegerVector createdTables, IntegerVector droppedTables) {
			// Add created tables to the committed tables list.
			for (int i = 0; i < createdTables.Count; ++i) {
				// For all created tables, add to the visible list and remove from the
				// delete list in the state store.
				MasterTableDataSource t = GetMasterTable(createdTables[i]);
				StateStore.StateResource resource = new StateStore.StateResource(t.TableID, CreateEncodedTableFile(t));
				stateStore.AddVisibleResource(resource);
				stateStore.RemoveDeleteResource(resource.name);
			}

			// Remove dropped tables from the committed tables list.
			for (int i = 0; i < droppedTables.Count; ++i) {
				// For all dropped tables, add to the delete list and remove from the
				// visible list in the state store.
				MasterTableDataSource t = GetMasterTable(droppedTables[i]);
				StateStore.StateResource resource = new StateStore.StateResource(t.TableID, CreateEncodedTableFile(t));
				stateStore.AddDeleteResource(resource);
				stateStore.RemoveVisibleResource(resource.name);
			}

			try {
				stateStore.Commit();
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}

		///<summary>
		/// Adds a listener for transactional modification events that occur on 
		/// the given table in this conglomerate.
		///</summary>
		///<param name="table_name">The name of the table in the conglomerate to 
		/// listen for events from.</param>
		///<param name="listener">The listener to be notified of events.</param>
		/// <remarks>
		/// A transactional modification event is an event fired immediately upon the 
		/// modification of a table by a transaction, either immediately before the 
		/// modification or immediately after.  Also an event is fired when a modification to 
		/// a table is successfully committed.
		/// <para>
		/// The BEFORE_* type triggers are given the opportunity to modify the contents 
		/// of the DataRow before the update or insert occurs.  All triggers may generate 
		/// an exception which will cause the transaction to rollback.
		/// </para>
		/// <para>
		/// The event carries with it the event type, the transaction that the event
		/// occurred in, and any information regarding the modification itself.
		/// </para>
		/// <para>
		/// This event/listener mechanism is intended to be used to implement higher
		/// layer database triggering systems.  Note that care must be taken with
		/// the commit level events because they occur inside a commit Lock on this
		/// conglomerate and so synchronization and deadlock issues need to be
		/// carefully considered.
		/// </para>
		/// <para>
		/// <b>Note</b>: A listener on the given table will be notified of ALL table
		/// modification events by all transactions at the time they happen.
		/// </para>
		/// </remarks>
		public void AddCommitModificationEventHandler(TableName table_name, CommitModificationEventHandler listener) {
			lock (modificationEvents) {
				modificationEvents.AddHandler(table_name, listener);
			}
		}

		/// <summary>
		/// Removes a listener for transaction modification events on the given table in 
		/// this conglomerate as previously set by the <see cref="AddCommitModificationEventHandler"/> 
		/// method.
		/// </summary>
		/// <param name="table_name">The name of the table in the conglomerate to remove 
		/// from the listener list.</param>
		/// <param name="listener">The listener to be removed.</param>
		public void RemoveCommitModificationEventHandler(TableName table_name, CommitModificationEventHandler listener) {
			lock (modificationEvents) {
				modificationEvents.RemoveHandler(table_name, listener);
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