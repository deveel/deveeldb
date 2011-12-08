// 
//  Copyright 2010  Deveel
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

namespace Deveel.Data {
	/// <summary>
	/// The list of all primitive operations to the database that a transaction
	/// performed.
	/// </summary>
	/// <remarks>
	/// It includes the list of all rows added or removed to all tables,
	/// and the tables created and dropped and any table that had constraint
	/// modifications.
	/// <para>
	/// This journal is updated inside a <see cref="Transaction"/>. When the 
	/// transaction is completed, this journal is used both to determine if the 
	/// transaction can be committed, and also to update the changes to the data 
	/// that a transaction has made.
	/// </para>
	/// <para>
	/// <b>Threading</b> The journal update commands are synchronized because 
	/// they need to be atomic operations and can be accessed by multiple threads.
	/// </para>
	/// </remarks>
	public sealed class TransactionJournal : IEnumerable<JournalCommand> {
		/// <summary>
		/// The number of entries in this journal.
		/// </summary>
		private int journalEntries;

		/// <summary>
		/// The list of table's that have been touched by this transaction.
		/// </summary>
		/// <remarks>
		/// A table is touched if the <see cref="SimpleTransaction.GetTable"/> method 
		/// in the transaction is used to get the table.  This means even if a table 
		/// is just read from, the journal will record that the table was touched.
		/// <para>
		/// This object records the 'table_id' of the touched tables in a sorted list.
		/// </para>
		/// </remarks>
		private readonly List<int> touchedTables;

		/// <summary>
		/// A byte[] array that represents the set of commands a transaction
		/// performed on a table.
		/// </summary>
		private byte[] commandJournal;

		/// <summary>
		/// An <see cref="IntegerVector"/> that is filled with parameters from the 
		/// command journal.
		/// </summary>
		/// <remarks>
		/// For example, a <see cref="JournalCommandType.AddRow"/> journal log will have 
		/// as parameters the table id the row was added to, and the row_index that was added.
		/// </remarks>
		private readonly List<int> commandParameters;

		// Optimization, these flags are set to true when various types of journal
		// entries are made to the transaction journal.
		private bool hasAddedTableRows,
		             hasRemovedTableRows,
		             hasCreatedTables,
		             hasDroppedTables,
		             hasConstraintAlterations;

		internal TransactionJournal() {
			journalEntries = 0;
			commandJournal = new byte[16];
			commandParameters = new List<int>(32);
			touchedTables = new List<int>(8);

			hasAddedTableRows = false;
			hasRemovedTableRows = false;
			hasCreatedTables = false;
			hasDroppedTables = false;
			hasConstraintAlterations = false;
		}

		/// <summary>
		/// Adds a command to the journal.
		/// </summary>
		/// <param name="command"></param>
		private void AddCommand(byte command) {
			if (journalEntries >= commandJournal.Length) {
				// Resize command array.
				int grow_size = System.Math.Min(4000, journalEntries);
				byte[] new_command_journal = new byte[journalEntries + grow_size];
				Array.Copy(commandJournal, 0, new_command_journal, 0,
				           journalEntries);
				commandJournal = new_command_journal;
			}

			commandJournal[journalEntries] = command;
			++journalEntries;
		}

		/// <summary>
		/// Adds a parameter to the journal command parameters.
		/// </summary>
		/// <param name="param"></param>
		private void AddParameter(int param) {
			commandParameters.Add(param);
		}

		/// <summary>
		/// Logs in this journal that the transaction touched the given table id.
		/// </summary>
		/// <param name="table_id"></param>
		internal void EntryAddTouchedTable(int table_id) {
			lock (this) {
				int pos = touchedTables.BinarySearch(table_id);
				// If table_id already in the touched table list.
				if (pos > 0 && touchedTables[pos] == table_id) {
					return;
				}
				// If position to insert < 0 set then add to the end of the set.
				if (pos < 0) {
					touchedTables.Add(table_id);
				} else {
					// Otherwise, insert into sorted order.
					touchedTables.Insert(pos, table_id);
				}
			}
		}

		/// <summary>
		/// Makes a journal entry that a table entry has been added to the table 
		/// with the given id.
		/// </summary>
		/// <param name="tableId"></param>
		/// <param name="rowIndex"></param>
		internal void EntryAddTableRow(int tableId, int rowIndex) {
			lock (this) {
				hasAddedTableRows = true;
				AddCommand((byte) JournalCommandType.AddRow);
				AddParameter(tableId);
				AddParameter(rowIndex);
			}
		}

		/// <summary>
		/// Makes a journal entry that a table entry has been removed from the 
		/// table with the given id.
		/// </summary>
		/// <param name="tableId"></param>
		/// <param name="rowIndex"></param>
		internal void EntryRemoveTableRow(int tableId, int rowIndex) {
			lock (this) {
				hasRemovedTableRows = true;
				AddCommand((byte) JournalCommandType.RemoveRow);
				AddParameter(tableId);
				AddParameter(rowIndex);
			}
		}

		/// <summary>
		/// Makes a journal entry that a table with the given 'table_id' has 
		/// been created by this transaction.
		/// </summary>
		/// <param name="tableId"></param>
		internal void EntryTableCreate(int tableId) {
			lock (this) {
				hasCreatedTables = true;
				AddCommand((byte) JournalCommandType.CreateTable);
				AddParameter(tableId);
			}
		}

		/// <summary>
		/// Makes a journal entry that a table with the given 'table_id' has 
		/// been dropped by this transaction.
		/// </summary>
		/// <param name="tableId"></param>
		internal void EntryTableDrop(int tableId) {
			lock (this) {
				hasDroppedTables = true;
				AddCommand((byte) JournalCommandType.DropTable);
				AddParameter(tableId);
			}
		}

		/// <summary>
		/// Makes a journal entry that a table with the given 'table_id' has 
		/// been altered by this transaction.
		/// </summary>
		/// <param name="tableId"></param>
		internal void EntryTableConstraintAlter(int tableId) {
			lock (this) {
				hasConstraintAlterations = true;
				AddCommand((byte) JournalCommandType.ConstraintAlter);
				AddParameter(tableId);
			}
		}


		/// <summary>
		/// Generates an array of <see cref="MasterTableJournal"/> objects that 
		/// specify the changes that occur to each table affected by this 
		/// transaction.
		/// </summary>
		/// <remarks>
		/// Each array element represents a change to an individual table in 
		/// the conglomerate that changed as a result of this transaction.
		/// <para>
		/// This is used when a transaction successfully commits and we need 
		/// to log the transaction changes with the master table.
		/// </para>
		/// <para>
		/// If no changes occurred to a table, then no entry is returned here.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal MasterTableJournal[] MakeMasterTableJournals() {
			List<MasterTableJournal> tableJournals = new List<MasterTableJournal>();
			int paramIndex = 0;

			MasterTableJournal masterJournal = null;

			for (int i = 0; i < journalEntries; ++i) {
				JournalCommandType c = (JournalCommandType) commandJournal[i];
				if (c == JournalCommandType.AddRow ||
				    c == JournalCommandType.RemoveRow) {
					int tableId = commandParameters[paramIndex];
					int rowIndex = commandParameters[paramIndex + 1];
					paramIndex += 2;

					// Do we already have this table journal?
					if (masterJournal == null ||
					    masterJournal.TableId != tableId) {
						// Try to find the journal in the list.
						int size = tableJournals.Count;
						masterJournal = null;
						for (int n = 0; n < size && masterJournal == null; ++n) {
							MasterTableJournal testJournal = tableJournals[n];
							if (testJournal.TableId == tableId)
								masterJournal = testJournal;
						}

						// Not found so add to list.
						if (masterJournal == null) {
							masterJournal = new MasterTableJournal(tableId);
							tableJournals.Add(masterJournal);
						}
					}

					// Add this change to the table journal.
					masterJournal.AddEntry(c, rowIndex);
				} else if (c == JournalCommandType.CreateTable ||
				           c == JournalCommandType.DropTable ||
				           c == JournalCommandType.ConstraintAlter) {
					paramIndex += 1;
				} else {
					throw new ApplicationException("Unknown journal command.");
				}
			}

			// Return the array.
			return tableJournals.ToArray();
		}

		/// <summary>
		/// Returns the list of tables id's that were dropped by this journal.
		/// </summary>
		/// <returns></returns>
		internal List<int> GetTablesDropped() {
			List<int> droppedTables = new List<int>();
			// Optimization, quickly return empty set if we know there are no tables.
			if (!hasDroppedTables)
				return droppedTables;

			int paramIndex = 0;
			for (int i = 0; i < journalEntries; ++i) {
				JournalCommandType c = (JournalCommandType) commandJournal[i];
				if (c == JournalCommandType.AddRow ||
				    c == JournalCommandType.RemoveRow) {
					paramIndex += 2;
				} else if (c == JournalCommandType.CreateTable ||
				           c == JournalCommandType.ConstraintAlter) {
					paramIndex += 1;
				} else if (c == JournalCommandType.DropTable) {
					droppedTables.Add(commandParameters[paramIndex]);
					paramIndex += 1;
				} else {
					throw new ApplicationException("Unknown journal command.");
				}
			}

			return droppedTables;
		}

		/// <summary>
		/// Returns the list of tables id's that were created by this journal.
		/// </summary>
		/// <returns></returns>
		internal List<int> GetTablesCreated() {
			List<int> createdTables = new List<int>();
			// Optimization, quickly return empty set if we know there are no tables.
			if (!hasCreatedTables)
				return createdTables;

			int paramIndex = 0;
			for (int i = 0; i < journalEntries; ++i) {
				JournalCommandType c = (JournalCommandType) commandJournal[i];
				if (c == JournalCommandType.AddRow ||
				    c == JournalCommandType.RemoveRow) {
					paramIndex += 2;
				} else if (c == JournalCommandType.DropTable ||
				           c == JournalCommandType.ConstraintAlter) {
					paramIndex += 1;
				} else if (c == JournalCommandType.CreateTable) {
					createdTables.Add(commandParameters[paramIndex]);
					paramIndex += 1;
				} else {
					throw new ApplicationException("Unknown journal command.");
				}
			}

			return createdTables;
		}

		/// <summary>
		/// Returns the list of tables id's that were constraint altered by 
		/// this journal.
		/// </summary>
		/// <returns></returns>
		internal List<int> GetTablesConstraintAltered() {
			List<int> calteredTables = new List<int>();
			// Optimization, quickly return empty set if we know there are no tables.
			if (!hasConstraintAlterations)
				return calteredTables;

			int paramIndex = 0;
			for (int i = 0; i < journalEntries; ++i) {
				JournalCommandType c = (JournalCommandType) commandJournal[i];
				if (c == JournalCommandType.AddRow ||
				    c == JournalCommandType.RemoveRow) {
					paramIndex += 2;
				} else if (c == JournalCommandType.DropTable ||
				           c == JournalCommandType.CreateTable) {
					paramIndex += 1;
				} else if (c == JournalCommandType.ConstraintAlter) {
					calteredTables.Add(commandParameters[paramIndex]);
					paramIndex += 1;
				} else {
					throw new ApplicationException("Unknown journal command.");
				}
			}

			return calteredTables;
		}

		public IEnumerator<JournalCommand> GetEnumerator() {
			return new CommandEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#region CommandEnumerator

		class CommandEnumerator : IEnumerator<JournalCommand> {
			private readonly TransactionJournal journal;
			private int index;
			private int entryCount;

			public CommandEnumerator(TransactionJournal journal) {
				this.journal = journal;
				index = -1;
				entryCount = journal.journalEntries;
			}

			private void AssertCoeherent() {
				if (entryCount != journal.journalEntries)
					throw new InvalidOperationException("The journal has changed: this enumeration is not coherent.");
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				AssertCoeherent();
				return ++index < entryCount;
			}

			public void Reset() {
				entryCount = journal.journalEntries;
				index = -1;
			}

			public JournalCommand Current {
				get {
					AssertCoeherent();
					JournalCommandType commandType = (JournalCommandType) journal.commandJournal[index];
					int tableId = journal.commandParameters[index];
					int rowIndex = -1;
					if (commandType == JournalCommandType.AddRow ||
						commandType == JournalCommandType.RemoveRow)
						rowIndex = journal.commandParameters[index + 1];

					return new JournalCommand(commandType, tableId, rowIndex);
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}