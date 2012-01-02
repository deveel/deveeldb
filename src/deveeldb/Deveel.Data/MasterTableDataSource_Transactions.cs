// 
//  Copyright 2010-2011  Deveel
// bs
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
using System.IO;

namespace Deveel.Data {
	public abstract partial class MasterTableDataSource {
		/// <summary>
		/// Updates the master records from the journal logs up to the given
		/// <paramref name="commitId"/>.
		/// </summary>
		/// <param name="commitId"></param>
		/// <remarks>
		/// This could be a fairly expensive operation if there are a lot of 
		/// modifications because each change could require a lookup of 
		/// records in the data source.
		/// <para>
		/// <b>Note</b>: It's extremely important that when this is called, 
		/// there are no transaction open that are using the merged journal. 
		/// If there is, then a transaction may be able to see changes in a 
		/// table that were made after the transaction started.
		/// </para>
		/// <para>
		/// After this method is called, it's best to update the index file
		/// with a call to 'synchronizeIndexFiles'
		/// </para>
		/// </remarks>
		internal void MergeJournalChanges(long commitId) {
			lock (this) {
				bool allMerged = tableIndices.MergeJournalChanges(commitId);
				// If all journal entries merged then schedule deleted row collection.
				if (allMerged && !IsReadOnly) {
					CheckForCleanup();
				}
			}
		}

		/// <summary>
		/// Returns a list of all <see cref="MasterTableJournal"/> objects that 
		/// have been successfully committed against this table that have an 
		/// <paramref name="commitId"/> that is greater or equal to the given.
		/// </summary>
		/// <param name="commitId"></param>
		/// <remarks>
		/// This is part of the conglomerate commit check phase and will be 
		/// on a commit_lock.
		/// </remarks>
		/// <returns></returns>
		internal MasterTableJournal[] FindAllJournalsSince(long commitId) {
			lock (this) {
				return tableIndices.FindAllJournalsSince(commitId);
			}
		}
		/// <summary>
		/// Adds a new transaction modification to this master table source.
		/// </summary>
		/// <param name="commitId"></param>
		/// <param name="change"></param>
		/// <param name="indexSet">Represents the changed index information to 
		/// commit to this table.</param>
		/// <remarks>
		/// This information represents the information that was added/removed 
		/// in the table in this transaction.
		/// <para>
		/// It's guarenteed that 'commit_id' additions will be sequential.
		/// </para>
		/// </remarks>
		internal void CommitTransactionChange(long commitId, MasterTableJournal change, IIndexSet indexSet) {
			lock (this) {
				// ASSERT: Can't do this if source is Read only.
				if (IsReadOnly)
					throw new ApplicationException("Can't commit transaction journal, table is Read only.");

				change.CommitId = commitId;

				try {

					// Add this journal to the multi version table indices log
					tableIndices.AddTransactionJournal(change);

					// Write the modified index set to the index store
					// (Updates the index file)
					CommitIndexSet(indexSet);

					// Update the state of the committed added data to the file system.
					// (Updates data to the allocation file)
					//
					// ISSUE: This can add up to a lot of changes to the allocation file and
					//   the runtime could potentially be terminated in the middle of
					//   the update.  If an interruption happens the allocation information
					//   may be incorrectly flagged.  The type of corruption this would
					//   result in would be;
					//   + From an 'update' the updated record may disappear.
					//   + From a 'delete' the deleted record may not delete.
					//   + From an 'insert' the inserted record may not insert.
					//
					// Note, the possibility of this type of corruption occuring has been
					// minimized as best as possible given the current architecture.
					// Also note that is not possible for a table file to become corrupted
					// beyond recovery from this issue.

					int size = change.EntriesCount;
					for (int i = 0; i < size; ++i) {
						JournalCommandType b = change.GetCommand(i);
						int rowIndex = change.GetRowIndex(i);
						// Was a row added or removed?
						if (b == JournalCommandType.AddRow) {
							// Record commit added
							int oldType = WriteRecordType(rowIndex, 0x010);
							// Check the record was in an uncommitted state before we changed
							// it.
							if ((oldType & 0x0F0) != 0) {
								WriteRecordType(rowIndex, oldType & 0x0F0);
								throw new ApplicationException("Record " + rowIndex + " of table " + this +
															   " was not in an uncommitted state!");
							}

						} else if (b == JournalCommandType.RemoveRow) {
							// Record commit removed
							int oldType = WriteRecordType(rowIndex, 0x020);
							// Check the record was in an added state before we removed it.
							if ((oldType & 0x0F0) != 0x010) {
								WriteRecordType(rowIndex, oldType & 0x0F0);
								throw new ApplicationException("Record " + rowIndex + " of table " + this +
															   " was not in an added state!");
							}
							// Notify collector that this row has been marked as deleted.
							gc.MarkRowAsDeleted(rowIndex);
						}
					}

				} catch (IOException e) {
					Logger.Error(this, e);
					throw new ApplicationException("IO Error: " + e.Message, e);
				}

			}
		}

		/// <summary>
		/// Rolls back a transaction change in this table source.
		/// </summary>
		/// <param name="change"></param>
		/// <remarks>
		/// Any rows added to the table will be uncommited rows (type_key = 0).  
		/// Those rows must be marked as committed deleted.
		/// </remarks>
		internal void RollbackTransactionChange(MasterTableJournal change) {
			lock (this) {
				// ASSERT: Can't do this is source is Read only.
				if (IsReadOnly)
					throw new ApplicationException("Can't rollback transaction journal, table is Read only.");

				// Any rows added in the journal are marked as committed deleted and the
				// journal is then discarded.

				try {
					// Mark all rows in the data_store as appropriate to the changes.
					int size = change.EntriesCount;
					for (int i = 0; i < size; ++i) {
						JournalCommandType b = change.GetCommand(i);
						int rowIndex = change.GetRowIndex(i);
						// Make row as added or removed.
						if (b == JournalCommandType.AddRow) {
							// Record commit removed (we are rolling back remember).
							//          int old_type = data_store.WriteRecordType(row_index + 1, 0x020);
							int oldType = WriteRecordType(rowIndex, 0x020);
							// Check the record was in an uncommitted state before we changed
							// it.
							if ((oldType & 0x0F0) != 0) {
								//            data_store.WriteRecordType(row_index + 1, old_type & 0x0F0);
								WriteRecordType(rowIndex, oldType & 0x0F0);
								throw new ApplicationException("Record " + rowIndex + " was not in an " +
															   "uncommitted state!");
							}
							// Notify collector that this row has been marked as deleted.
							gc.MarkRowAsDeleted(rowIndex);
						} else if (b == JournalCommandType.RemoveRow) {
							// Any journal entries marked as TABLE_REMOVE are ignored because
							// we are rolling back.  This means the row is not logically changed.
						}
					}

					// The journal entry is discarded, the indices do not need to be updated
					// to reflect this rollback.
				} catch (IOException e) {
					Logger.Error(this, e);
					throw new ApplicationException("IO Error: " + e.Message, e);
				}
			}
		}

		/// <summary>
		/// Returns a <see cref="IMutableTableDataSource"/> object that represents 
		/// this data source at the time the given transaction started.
		/// </summary>
		/// <param name="transaction"></param>
		/// <remarks>
		/// Any modifications to the returned table are logged in the table 
		/// journal.
		/// <para>
		/// This is a key method in this object because it allows us to get a 
		/// data source that represents the data in the table before any 
		/// modifications may have been committed.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal IMutableTableDataSource CreateTableDataSourceAtCommit(SimpleTransaction transaction) {
			return CreateTableDataSourceAtCommit(transaction, new MasterTableJournal(TableId));
		}

		/// <summary>
		/// Returns a <see cref="IMutableTableDataSource"/> object that represents 
		/// this data source at the time the given transaction started, and also 
		/// makes any modifications that are described by the journal in the 
		/// table.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="journal"></param>
		/// <remarks>
		/// This method is useful for merging the changes made by a transaction 
		/// into a view of the table.
		/// </remarks>
		/// <returns></returns>
		internal IMutableTableDataSource CreateTableDataSourceAtCommit(SimpleTransaction transaction, MasterTableJournal journal) {
			return new MMutableTableDataSource(this, transaction, journal);
		}

		/// <summary>
		/// A <see cref="IMutableTableDataSource"/> object as returned by the 
		/// <see cref="MasterTableDataSource.CreateTableDataSourceAtCommit(SimpleTransaction,MasterTableJournal)"/> 
		/// method.
		/// </summary>
		/// <remarks>
		/// <b>Note</b> This object is <b>not</b> thread-safe and it is assumed
		/// any use of this object will be thread exclusive. This is okay because 
		/// multiple instances of this object can be created on the same 
		/// <see cref="MasterTableDataSource"/> if multi-thread access to a 
		/// <see cref="MasterTableDataSource"/> is desirable.
		/// </remarks>
		private sealed class MMutableTableDataSource : IMutableTableDataSource {
			private readonly MasterTableDataSource mtds;

			/// <summary>
			///The Transaction object that this IMutableTableDataSource was
			/// generated from.
			/// </summary>
			/// <remarks>
			/// This reference should be used only to query database constraint 
			/// information.
			/// </remarks>
			private SimpleTransaction transaction;

			/// <summary>
			/// True if the transaction is Read-only.
			/// </summary>
			private readonly bool tranReadOnly;

			/// <summary>
			/// The name of this table.
			/// </summary>
			private TableName tableName;

			/// <summary>
			/// The 'recovery point' to which the row index in this table source 
			/// has rebuilt to.
			/// </summary>
			private int rowListRebuild;

			/// <summary>
			/// The index that represents the rows that are within this
			/// table data source within this transaction.
			/// </summary>
			private IIndex rowList;

			/// <summary>
			/// The 'recovery point' to which the schemes in this table source have
			/// rebuilt to.
			/// </summary>
			private int[] schemeRebuilds;

			/// <summary>
			/// The IIndexSet for this mutable table source.
			/// </summary>
			private IIndexSet indexSet;

			/// <summary>
			/// The SelectableScheme array that represents the schemes for the
			/// columns within this transaction.
			/// </summary>
			private readonly SelectableScheme[] columnSchemes;

			/// <summary>
			/// A journal of changes to this source since it was created.
			/// </summary>
			private MasterTableJournal tableJournal;

			/// <summary>
			/// The last time any changes to the journal were check for referential
			/// integrity violations.
			/// </summary>
			private int lastEntryRICheck;

			public MMutableTableDataSource(MasterTableDataSource mtds, SimpleTransaction transaction, MasterTableJournal journal) {
				this.mtds = mtds;
				this.transaction = transaction;
				indexSet = transaction.GetIndexSetForTable(mtds);
				int colCount = TableInfo.ColumnCount;
				tableName = TableInfo.TableName;
				tranReadOnly = transaction.IsReadOnly;
				rowListRebuild = 0;
				schemeRebuilds = new int[colCount];
				columnSchemes = new SelectableScheme[colCount];
				tableJournal = journal;
				lastEntryRICheck = tableJournal.EntriesCount;
			}

			/// <summary>
			/// Executes an update referential action.
			/// </summary>
			/// <param name="constraint"></param>
			/// <param name="originalKey"></param>
			/// <param name="newKey"></param>
			/// <param name="context"></param>
			/// <exception cref="ApplicationException">
			/// If the update action is "NO ACTION", and the constraint is 
			/// <see cref="ConstraintDeferrability.InitiallyImmediate"/>, and 
			/// the new key doesn't exist in the referral table.
			/// </exception>
			private void ExecuteUpdateReferentialAction(DataConstraintInfo constraint, TObject[] originalKey, TObject[] newKey, IQueryContext context) {
				ConstraintAction update_rule = constraint.UpdateRule;
				if (update_rule == ConstraintAction.NoAction &&
					constraint.Deferred != ConstraintDeferrability.InitiallyImmediate) {
					// Constraint check is deferred
					return;
				}

				// So either update rule is not NO ACTION, or if it is we are initially
				// immediate.
				IMutableTableDataSource keyTable = transaction.GetMutableTable(constraint.TableName);
				DataTableInfo tableInfo = keyTable.TableInfo;
				int[] keyCols = TableDataConglomerate.FindColumnIndices(tableInfo, constraint.Columns);
				IList<int> keyEntries = TableDataConglomerate.FindKeys(keyTable, keyCols, originalKey);

				// Are there keys effected?
				if (keyEntries.Count > 0) {
					if (update_rule == ConstraintAction.NoAction)
						// Throw an exception;
						throw new DatabaseConstraintViolationException(
							DatabaseConstraintViolationException.ForeignKeyViolation,
							TableDataConglomerate.DeferredString(constraint.Deferred) +
							" foreign key constraint violation on update (" +
							constraint.Name + ") Columns = " +
							constraint.TableName + "( " +
							TableDataConglomerate.StringColumnList(constraint.Columns) +
							" ) -> " + constraint.ReferencedTableName + "( " +
							TableDataConglomerate.StringColumnList(constraint.ReferencedColumns) +
							" )");

					// Perform a referential action on each updated key
					foreach (int rowIndex in keyEntries) {
						DataRow dataRow = new DataRow(keyTable);
						dataRow.SetFromRow(rowIndex);

						if (update_rule == ConstraintAction.Cascade) {
							// Update the keys
							for (int n = 0; n < keyCols.Length; ++n) {
								dataRow.SetValue(keyCols[n], newKey[n]);
							}
							keyTable.UpdateRow(rowIndex, dataRow);
						} else if (update_rule == ConstraintAction.SetNull) {
							for (int n = 0; n < keyCols.Length; ++n) {
								dataRow.SetToNull(keyCols[n]);
							}
							keyTable.UpdateRow(rowIndex, dataRow);
						} else if (update_rule == ConstraintAction.SetDefault) {
							for (int n = 0; n < keyCols.Length; ++n) {
								dataRow.SetToDefault(keyCols[n], context);
							}
							keyTable.UpdateRow(rowIndex, dataRow);
						} else {
							throw new Exception("Do not understand referential action: " + update_rule);
						}
					}
					// Check referential integrity of modified table,
					keyTable.ConstraintIntegrityCheck();
				}
			}

			/// <summary>
			/// Executes a delete referential action.
			/// </summary>
			/// <param name="constraint"></param>
			/// <param name="originalKey"></param>
			/// <param name="context"></param>
			/// <exception cref="ApplicationException">
			/// If the delete action is "NO ACTION", and the constraint is 
			/// <see cref="ConstraintDeferrability.InitiallyImmediate"/>, and 
			/// the new key doesn't exist in the referral table.
			/// </exception>
			private void ExecuteDeleteReferentialAction(DataConstraintInfo constraint, TObject[] originalKey, IQueryContext context) {
				ConstraintAction delete_rule = constraint.DeleteRule;
				if (delete_rule == ConstraintAction.NoAction &&
					constraint.Deferred != ConstraintDeferrability.InitiallyImmediate) {
					// Constraint check is deferred
					return;
				}

				// So either delete rule is not NO ACTION, or if it is we are initially
				// immediate.
				IMutableTableDataSource keyTable = transaction.GetMutableTable(constraint.TableName);
				DataTableInfo tableInfo = keyTable.TableInfo;
				int[] keyCols = TableDataConglomerate.FindColumnIndices(tableInfo, constraint.Columns);
				IList<int> keyEntries = TableDataConglomerate.FindKeys(keyTable, keyCols, originalKey);

				// Are there keys effected?
				if (keyEntries.Count > 0) {
					if (delete_rule == ConstraintAction.NoAction) {
						// Throw an exception;
						throw new DatabaseConstraintViolationException(
							DatabaseConstraintViolationException.ForeignKeyViolation,
							TableDataConglomerate.DeferredString(constraint.Deferred) +
							" foreign key constraint violation on delete (" +
							constraint.Name + ") Columns = " +
							constraint.TableName + "( " +
							TableDataConglomerate.StringColumnList(constraint.Columns) +
							" ) -> " + constraint.ReferencedTableName + "( " +
							TableDataConglomerate.StringColumnList(constraint.ReferencedColumns) +
							" )");
					}

					// Perform a referential action on each updated key
					foreach (int rowIndex in keyEntries) {
						DataRow dataRow = new DataRow(keyTable);
						dataRow.SetFromRow(rowIndex);
						if (delete_rule == ConstraintAction.Cascade) {
							// Cascade the removal of the referenced rows
							keyTable.RemoveRow(rowIndex);
						} else if (delete_rule == ConstraintAction.SetNull) {
							for (int n = 0; n < keyCols.Length; ++n) {
								dataRow.SetToNull(keyCols[n]);
							}
							keyTable.UpdateRow(rowIndex, dataRow);
						} else if (delete_rule == ConstraintAction.SetDefault) {
							for (int n = 0; n < keyCols.Length; ++n) {
								dataRow.SetToDefault(keyCols[n], context);
							}
							keyTable.UpdateRow(rowIndex, dataRow);
						} else {
							throw new Exception("Do not understand referential action: " + delete_rule);
						}
					}
					// Check referential integrity of modified table,
					keyTable.ConstraintIntegrityCheck();
				}
			}

			/// <summary>
			/// Returns the entire row list for this table.
			/// </summary>
			/// <remarks>
			/// This will request this information from the master source.
			/// </remarks>
			private IIndex RowIndex {
				get {
					if (rowList == null) {
						rowList = indexSet.GetIndex(0);
					}
					return rowList;
				}
			}

			/// <summary>
			/// Ensures that the row list is as current as the latest journal change.
			/// </summary>
			/// <remarks>
			/// We can be assured that when this is called, no journal changes 
			/// will occur concurrently. However we still need to synchronize 
			/// because multiple reads are valid.
			/// </remarks>
			private void EnsureRowIndexListCurrent() {
				int rebuildIndex = rowListRebuild;
				int journalCount = tableJournal.EntriesCount;
				while (rebuildIndex < journalCount) {
					JournalCommandType command = tableJournal.GetCommand(rebuildIndex);
					int rowIndex = tableJournal.GetRowIndex(rebuildIndex);
					if (command == JournalCommandType.AddRow ||
						command == JournalCommandType.UpdateAddRow) {
						// Add to 'row_list'.
						if (!RowIndex.UniqueInsertSort(rowIndex))
							throw new ApplicationException("Row index already used in this table (" + rowIndex + ")");
					} else if (command == JournalCommandType.RemoveRow ||
							   command == JournalCommandType.UpdateRemoveRow) {
						// Remove from 'row_list'
						if (!RowIndex.RemoveSort(rowIndex))
							throw new ApplicationException("Row index removed that wasn't in this table!");
					} else {
						throw new ApplicationException("Unrecognised journal command.");
					}
					++rebuildIndex;
				}
				// It's now current (row_list_rebuild == journal_count);
				rowListRebuild = rebuildIndex;
			}

			/// <summary>
			/// Ensures that the scheme column index is as current as the latest
			/// journal change.
			/// </summary>
			/// <param name="column"></param>
			private void EnsureColumnSchemeCurrent(int column) {
				SelectableScheme scheme = columnSchemes[column];
				// NOTE: We should be assured that no Write operations can occur over
				//   this section of code because writes are exclusive operations
				//   within a transaction.
				// Are there journal entries pending on this scheme since?
				int rebuildIndex = schemeRebuilds[column];
				int journalCount = tableJournal.EntriesCount;
				while (rebuildIndex < journalCount) {
					JournalCommandType command = tableJournal.GetCommand(rebuildIndex);
					int row_index = tableJournal.GetRowIndex(rebuildIndex);
					if (command == JournalCommandType.AddRow ||
						command == JournalCommandType.UpdateAddRow) {
						scheme.Insert(row_index);
					} else if (command == JournalCommandType.RemoveRow ||
							   command == JournalCommandType.UpdateRemoveRow) {
						scheme.Remove(row_index);
					} else {
						throw new ApplicationException("Unrecognised journal command.");
					}
					++rebuildIndex;
				}
				schemeRebuilds[column] = rebuildIndex;
			}

			// ---------- Implemented from IMutableTableDataSource ----------

			public TransactionSystem System {
				get { return mtds.System; }
			}

			public DataTableInfo TableInfo {
				get { return mtds.TableInfo; }
			}

			public int RowCount {
				get {
					// Ensure the row list is up to date.
					EnsureRowIndexListCurrent();
					return RowIndex.Count;
				}
			}

			public IRowEnumerator GetRowEnumerator() {
				// Ensure the row list is up to date.
				EnsureRowIndexListCurrent();
				// Get an iterator across the row list.
				IIndexEnumerator iterator = RowIndex.GetEnumerator();
				// Wrap it around a IRowEnumerator object.
				return new RowEnumerator(iterator);
			}

			public TObject GetCellContents(int column, int row) {
				return mtds.GetCellContents(column, row);
			}

			// NOTE: Returns an immutable version of the scheme...
			public SelectableScheme GetColumnScheme(int column) {
				SelectableScheme scheme = columnSchemes[column];
				// Cache the scheme in this object.
				if (scheme == null) {
					scheme = mtds.CreateSelectableSchemeForColumn(indexSet, this, column);
					columnSchemes[column] = scheme;
				}

				// Update the underlying scheme to the most current version.
				EnsureColumnSchemeCurrent(column);

				return scheme;
			}

			// ---------- Table Modification ----------

			public int AddRow(DataRow dataRow) {

				// Check the transaction isn't Read only.
				if (tranReadOnly) {
					throw new Exception("Transaction is Read only.");
				}

				// Check this isn't a Read only source
				if (mtds.IsReadOnly) {
					throw new ApplicationException("Can not add row - table is Read only.");
				}

				// Add to the master.
				int rowIndex;
				try {
					rowIndex = mtds.AddRow(dataRow);
				} catch (IOException e) {
					mtds.Logger.Error(this, e);
					throw new ApplicationException("IO Error: " + e.Message, e);
				}

				// Note this doesn't need to be synchronized because we are exclusive on
				// this table.
				// Add this change to the table journal.
				tableJournal.AddEntry(JournalCommandType.AddRow, rowIndex);

				return rowIndex;
			}

			public void RemoveRow(int rowIndex) {

				// Check the transaction isn't Read only.
				if (tranReadOnly) {
					throw new Exception("Transaction is Read only.");
				}

				// Check this isn't a Read only source
				if (mtds.IsReadOnly) {
					throw new ApplicationException("Can not remove row - table is Read only.");
				}

				// NOTE: This must <b>NOT</b> call 'RemoveRow' in MasterTableDataSource.
				//   We do not want to delete a row permanently from the underlying
				//   file because the transaction using this data source may yet decide
				//   to roll back the change and not delete the row.

				// Note this doesn't need to be synchronized because we are exclusive on
				// this table.
				// Add this change to the table journal.
				tableJournal.AddEntry(JournalCommandType.RemoveRow, rowIndex);

			}

			public int UpdateRow(int rowIndex, DataRow dataRow) {

				// Check the transaction isn't Read only.
				if (tranReadOnly) {
					throw new Exception("Transaction is Read only.");
				}

				// Check this isn't a Read only source
				if (mtds.IsReadOnly) {
					throw new ApplicationException("Can not update row - table is Read only.");
				}

				// Note this doesn't need to be synchronized because we are exclusive on
				// this table.
				// Add this change to the table journal.
				tableJournal.AddEntry(JournalCommandType.UpdateRemoveRow, rowIndex);

				// Add to the master.
				int new_row_index;
				try {
					new_row_index = mtds.AddRow(dataRow);
				} catch (IOException e) {
					mtds.Logger.Error(this, e);
					throw new ApplicationException("IO Error: " + e.Message, e);
				}

				// Note this doesn't need to be synchronized because we are exclusive on
				// this table.
				// Add this change to the table journal.
				tableJournal.AddEntry(JournalCommandType.UpdateAddRow, new_row_index);

				return new_row_index;
			}


			public void FlushIndexChanges() {
				EnsureRowIndexListCurrent();
				// This will flush all of the column schemes
				for (int i = 0; i < columnSchemes.Length; ++i) {
					GetColumnScheme(i);
				}
			}

			public void ConstraintIntegrityCheck() {
				try {
					// Early exit condition
					if (lastEntryRICheck == tableJournal.EntriesCount)
						return;

					// This table name
					DataTableInfo tableInfo = TableInfo;
					TableName tName = tableInfo.TableName;
					IQueryContext context = new SystemQueryContext(transaction, tName.Schema);

					// Are there any added, deleted or updated entries in the journal since
					// we last checked?
					List<int> rowsUpdated = new List<int>();
					List<int> rowsDeleted = new List<int>();
					List<int> rowsAdded = new List<int>();

					int size = tableJournal.EntriesCount;
					for (int i = lastEntryRICheck; i < size; ++i) {
						JournalCommandType tc = tableJournal.GetCommand(i);
						int rowIndex = tableJournal.GetRowIndex(i);
						if (tc == JournalCommandType.RemoveRow ||
							tc == JournalCommandType.UpdateRemoveRow) {
							rowsDeleted.Add(rowIndex);
							// If this is in the rows_added list, remove it from rows_added
							int raI = rowsAdded.IndexOf(rowIndex);
							if (raI != -1)
								rowsAdded.RemoveAt(raI);
						} else if (tc == JournalCommandType.AddRow ||
								   tc == JournalCommandType.UpdateAddRow) {
							rowsAdded.Add(rowIndex);
						}

						if (tc == JournalCommandType.UpdateRemoveRow) {
							rowsUpdated.Add(rowIndex);
						} else if (tc == JournalCommandType.UpdateAddRow) {
							rowsUpdated.Add(rowIndex);
						}
					}

					// Were there any updates or deletes?
					if (rowsDeleted.Count > 0) {
						// Get all references on this table
						DataConstraintInfo[] foreignConstraints = Transaction.QueryTableImportedForeignKeys(transaction, tName);

						// For each foreign constraint
						foreach (DataConstraintInfo constraint in foreignConstraints) {
							// For each deleted/updated record in the table,
							foreach (int rowIndex in rowsDeleted) {
								// What was the key before it was updated/deleted
								int[] cols = TableDataConglomerate.FindColumnIndices(tableInfo, constraint.ReferencedColumns);
								TObject[] originalKey = new TObject[cols.Length];
								int nullCount = 0;
								for (int p = 0; p < cols.Length; ++p) {
									originalKey[p] = GetCellContents(cols[p], rowIndex);
									if (originalKey[p].IsNull) {
										++nullCount;
									}
								}
								// Check the original key isn't null
								if (nullCount != cols.Length) {
									// Is is an update?
									int updateIndex = rowsUpdated.IndexOf(rowIndex);
									if (updateIndex != -1) {
										// Yes, this is an update
										int rowIndexAdd = rowsUpdated[updateIndex + 1];
										// It must be an update, so first see if the change caused any
										// of the keys to change.
										bool keyChanged = false;
										TObject[] keyUpdatedTo = new TObject[cols.Length];
										for (int p = 0; p < cols.Length; ++p) {
											keyUpdatedTo[p] = GetCellContents(cols[p], rowIndexAdd);
											if (originalKey[p].CompareTo(keyUpdatedTo[p]) != 0) {
												keyChanged = true;
											}
										}
										if (keyChanged) {
											// Allow the delete, and execute the action,
											// What did the key update to?
											ExecuteUpdateReferentialAction(constraint, originalKey, keyUpdatedTo, context);
										}
										// If the key didn't change, we don't need to do anything.
									} else {
										// No, so it must be a delete,
										// This will look at the referencee table and if it contains
										// the key, work out what to do with it.
										ExecuteDeleteReferentialAction(constraint, originalKey, context);
									}

								}  // If the key isn't null

							}  // for each deleted rows

						}  // for each foreign key reference to this table

					}

					// Were there any rows added (that weren't deleted)?
					if (rowsAdded.Count > 0) {
						int[] rowIndices = rowsAdded.ToArray();

						// Check for any field constraint violations in the added rows
						TableDataConglomerate.CheckFieldConstraintViolations(transaction, this, rowIndices);
						// Check this table, adding the given row_index, immediate
						TableDataConglomerate.CheckAddConstraintViolations(transaction, this, rowIndices, ConstraintDeferrability.InitiallyImmediate);
					}
				} catch (DatabaseConstraintViolationException) {
					// If a constraint violation, roll back the changes since the last
					// check.
					int rollbackPoint = tableJournal.EntriesCount - lastEntryRICheck;
					if (rowListRebuild <= rollbackPoint) {
						tableJournal.RollbackEntries(rollbackPoint);
					} else {
						Console.Out.WriteLine(
						   "Warning: rebuild_pointer is after rollback point so we can't " +
						   "rollback to the point before the constraint violation.");
					}

					throw;
				} finally {
					// Make sure we update the 'last_entry_ri_check' variable
					lastEntryRICheck = tableJournal.EntriesCount;
				}

			}

			public MasterTableJournal Journal {
				get { return tableJournal; }
			}

			public void Dispose() {
				// Dispose and invalidate the schemes
				// This is really a safety measure to ensure the schemes can't be
				// used outside the scope of the lifetime of this object.
				for (int i = 0; i < columnSchemes.Length; ++i) {
					SelectableScheme scheme = columnSchemes[i];
					if (scheme != null) {
						scheme.Dispose();
						columnSchemes[i] = null;
					}
				}
				rowList = null;
				tableJournal = null;
				schemeRebuilds = null;
				indexSet = null;
				transaction = null;
			}

			public void AddRootLock() {
				mtds.AddRootLock();
			}

			public void RemoveRootLock() {
				mtds.RemoveRootLock();
			}

			private class RowEnumerator : IRowEnumerator {
				public RowEnumerator(IIndexEnumerator baseEnumerator) {
					this.baseEnumerator = baseEnumerator;
				}

				private readonly IIndexEnumerator baseEnumerator;

				public bool MoveNext() {
					return baseEnumerator.MoveNext();
				}

				public void Reset() {
				}

				public object Current {
					get { return RowIndex; }
				}

				public int RowIndex {
					get { return baseEnumerator.Current; }
				}
			}
		}
	}
}