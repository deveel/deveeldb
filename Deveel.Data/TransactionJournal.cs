// 
//  TransactionJournal.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

using Deveel.Data.Collections;

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
	sealed class TransactionJournal {

		/**
		 * Journal commands.
		 */
		internal const byte TABLE_ADD = 1;  // Add a row to a table.
		// (params: table_id, row_index)
		internal const byte TABLE_REMOVE = 2;  // Remove a row from a table.
		// (params: table_id, row_index)
		internal const byte TABLE_CREATE = 3;  // Create a new table.
		// (params: table_id)
		internal const byte TABLE_DROP = 4;  // Drop a table.
		// (params: table_id)
		internal const byte TABLE_CONSTRAINT_ALTER = 5; // Alter constraints of a table.
		// (params: table_id)

		/// <summary>
		/// The number of entries in this journal.
		/// </summary>
		private int journal_entries;

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
		private readonly IntegerVector touched_tables;

		/// <summary>
		/// A byte[] array that represents the set of commands a transaction
		/// performed on a table.
		/// </summary>
		private byte[] command_journal;

		/// <summary>
		/// An <see cref="IntegerVector"/> that is filled with parameters from the 
		/// command journal.
		/// </summary>
		/// <remarks>
		/// For example, a <see cref="TABLE_ADD"/> journal log will have as parameters 
		/// the table id the row was added to, and the row_index that was added.
		/// </remarks>
		private readonly IntegerVector command_parameters;

		// Optimization, these flags are set to true when various types of journal
		// entries are made to the transaction journal.
		private bool has_added_table_rows, has_removed_table_rows,
				 has_created_tables, has_dropped_tables, has_constraint_alterations;

		internal TransactionJournal() {
			journal_entries = 0;
			command_journal = new byte[16];
			command_parameters = new IntegerVector(32);
			touched_tables = new IntegerVector(8);

			has_added_table_rows = false;
			has_removed_table_rows = false;
			has_created_tables = false;
			has_dropped_tables = false;
			has_constraint_alterations = false;
		}

		/// <summary>
		/// Adds a command to the journal.
		/// </summary>
		/// <param name="command"></param>
		private void AddCommand(byte command) {
			if (journal_entries >= command_journal.Length) {
				// Resize command array.
				int grow_size = System.Math.Min(4000, journal_entries);
				byte[] new_command_journal = new byte[journal_entries + grow_size];
				Array.Copy(command_journal, 0, new_command_journal, 0,
								 journal_entries);
				command_journal = new_command_journal;
			}

			command_journal[journal_entries] = command;
			++journal_entries;
		}

		/// <summary>
		/// Adds a parameter to the journal command parameters.
		/// </summary>
		/// <param name="param"></param>
		private void AddParameter(int param) {
			command_parameters.AddInt(param);
		}

		/// <summary>
		/// Logs in this journal that the transaction touched the given table id.
		/// </summary>
		/// <param name="table_id"></param>
		internal void EntryAddTouchedTable(int table_id) {
			lock (this) {
				int pos = touched_tables.SortedIndexOf(table_id);
				// If table_id already in the touched table list.
				if (pos < touched_tables.Count &&
					touched_tables[pos] == table_id) {
					return;
				}
				// If position to insert >= size of the touched tables set then add to
				// the end of the set.
				if (pos >= touched_tables.Count) {
					touched_tables.AddInt(table_id);
				} else {
					// Otherwise, insert into sorted order.
					touched_tables.InsertIntAt(table_id, pos);
				}
			}
		}

		/// <summary>
		/// Makes a journal entry that a table entry has been added to the table 
		/// with the given id.
		/// </summary>
		/// <param name="table_id"></param>
		/// <param name="row_index"></param>
		internal void EntryAddTableRow(int table_id, int row_index) {
			lock (this) {
				//    has_added_table_rows = true;
				AddCommand(TABLE_ADD);
				AddParameter(table_id);
				AddParameter(row_index);
			}
		}

		/// <summary>
		/// Makes a journal entry that a table entry has been removed from the 
		/// table with the given id.
		/// </summary>
		/// <param name="table_id"></param>
		/// <param name="row_index"></param>
		internal void EntryRemoveTableRow(int table_id, int row_index) {
			lock (this) {
				//    has_removed_table_rows = true;
				AddCommand(TABLE_REMOVE);
				AddParameter(table_id);
				AddParameter(row_index);
			}
		}

		/// <summary>
		/// Makes a journal entry that a table with the given 'table_id' has 
		/// been created by this transaction.
		/// </summary>
		/// <param name="table_id"></param>
		internal void EntryTableCreate(int table_id) {
			lock (this) {
				has_created_tables = true;
				AddCommand(TABLE_CREATE);
				AddParameter(table_id);
			}
		}

		/// <summary>
		/// Makes a journal entry that a table with the given 'table_id' has 
		/// been dropped by this transaction.
		/// </summary>
		/// <param name="table_id"></param>
		internal void EntryTableDrop(int table_id) {
			lock (this) {
				has_dropped_tables = true;
				AddCommand(TABLE_DROP);
				AddParameter(table_id);
			}
		}

		/// <summary>
		/// Makes a journal entry that a table with the given 'table_id' has 
		/// been altered by this transaction.
		/// </summary>
		/// <param name="table_id"></param>
		internal void EntryTableConstraintAlter(int table_id) {
			lock (this) {
				has_constraint_alterations = true;
				AddCommand(TABLE_CONSTRAINT_ALTER);
				AddParameter(table_id);
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
			ArrayList table_journals = new ArrayList();
			int param_index = 0;

			MasterTableJournal master_journal = null;

			for (int i = 0; i < journal_entries; ++i) {
				byte c = command_journal[i];
				if (c == TABLE_ADD || c == TABLE_REMOVE) {
					int table_id = command_parameters[param_index];
					int row_index = command_parameters[param_index + 1];
					param_index += 2;

					// Do we already have this table journal?
					if (master_journal == null ||
						master_journal.TableId != table_id) {
						// Try to find the journal in the list.
						int size = table_journals.Count;
						master_journal = null;
						for (int n = 0; n < size && master_journal == null; ++n) {
							MasterTableJournal test_journal =
												   (MasterTableJournal)table_journals[n];
							if (test_journal.TableId == table_id) {
								master_journal = test_journal;
							}
						}

						// Not found so add to list.
						if (master_journal == null) {
							master_journal = new MasterTableJournal(table_id);
							table_journals.Add(master_journal);
						}

					}

					// Add this change to the table journal.
					master_journal.AddEntry(c, row_index);

				} else if (c == TABLE_CREATE ||
						 c == TABLE_DROP ||
						 c == TABLE_CONSTRAINT_ALTER) {
					param_index += 1;
				} else {
					throw new ApplicationException("Unknown journal command.");
				}
			}

			// Return the array.
			return (MasterTableJournal[])table_journals.ToArray(typeof(MasterTableJournal));

		}

		/// <summary>
		/// Returns the list of tables id's that were dropped by this journal.
		/// </summary>
		/// <returns></returns>
		internal IntegerVector GetTablesDropped() {
			IntegerVector dropped_tables = new IntegerVector();
			// Optimization, quickly return empty set if we know there are no tables.
			if (!has_dropped_tables) {
				return dropped_tables;
			}

			int param_index = 0;
			for (int i = 0; i < journal_entries; ++i) {
				byte c = command_journal[i];
				if (c == TABLE_ADD || c == TABLE_REMOVE) {
					param_index += 2;
				} else if (c == TABLE_CREATE || c == TABLE_CONSTRAINT_ALTER) {
					param_index += 1;
				} else if (c == TABLE_DROP) {
					dropped_tables.AddInt(command_parameters[param_index]);
					param_index += 1;
				} else {
					throw new ApplicationException("Unknown journal command.");
				}
			}

			return dropped_tables;
		}

		/// <summary>
		/// Returns the list of tables id's that were created by this journal.
		/// </summary>
		/// <returns></returns>
		internal IntegerVector GetTablesCreated() {
			IntegerVector created_tables = new IntegerVector();
			// Optimization, quickly return empty set if we know there are no tables.
			if (!has_created_tables) {
				return created_tables;
			}

			int param_index = 0;
			for (int i = 0; i < journal_entries; ++i) {
				byte c = command_journal[i];
				if (c == TABLE_ADD || c == TABLE_REMOVE) {
					param_index += 2;
				} else if (c == TABLE_DROP || c == TABLE_CONSTRAINT_ALTER) {
					param_index += 1;
				} else if (c == TABLE_CREATE) {
					created_tables.AddInt(command_parameters[param_index]);
					param_index += 1;
				} else {
					throw new ApplicationException("Unknown journal command.");
				}
			}

			return created_tables;
		}

		/// <summary>
		/// Returns the list of tables id's that were constraint altered by 
		/// this journal.
		/// </summary>
		/// <returns></returns>
		internal IntegerVector GetTablesConstraintAltered() {
			IntegerVector caltered_tables = new IntegerVector();
			// Optimization, quickly return empty set if we know there are no tables.
			if (!has_constraint_alterations) {
				return caltered_tables;
			}

			int param_index = 0;
			for (int i = 0; i < journal_entries; ++i) {
				byte c = command_journal[i];
				if (c == TABLE_ADD || c == TABLE_REMOVE) {
					param_index += 2;
				} else if (c == TABLE_DROP || c == TABLE_CREATE) {
					param_index += 1;
				} else if (c == TABLE_CONSTRAINT_ALTER) {
					caltered_tables.AddInt(command_parameters[param_index]);
					param_index += 1;
				} else {
					throw new ApplicationException("Unknown journal command.");
				}
			}

			return caltered_tables;
		}
	}
}