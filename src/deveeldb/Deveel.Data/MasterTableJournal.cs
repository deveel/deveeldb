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
using System.Text;

using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// A journal of changes that occured to a table in a data conglomerate during
	/// a transaction.
	/// </summary>
	public class MasterTableJournal {
		/// <summary>
		/// The commit id given to this change when it is committed.
		/// </summary>
		/// <remarks>
		/// This is only set when the journal is a committed change to the database.
		/// </remarks>
		private long commit_id;


		/// <summary>
		/// The master table id.
		/// </summary>
		private int table_id;

		/// <summary>
		/// The number of entries in this journal.
		/// </summary>
		private int journal_entries;

		/// <summary>
		/// A byte[] array that represents the set of commands a transaction
		/// performed on this table.
		/// </summary>
		private byte[] command_journal;

		/// <summary>
		/// An IntegerVector that is filled with parameters from the command journal.
		/// </summary>
		/// <remarks>
		/// For example, a 'TABLE_ADD' journal log will have as parameters the
		/// row_index that was added to this table.
		/// </remarks>
		private readonly IntegerVector command_parameters;

		internal MasterTableJournal(int table_id) {
			this.table_id = table_id;
			command_journal = new byte[16];
			command_parameters = new IntegerVector(32);
		}

		internal MasterTableJournal()
			: this(-1) {
		}

		/// <summary>
		/// Returns true if the given command is an addition command.
		/// </summary>
		/// <param name="command"></param>
		/// <returns></returns>
		internal static bool IsAddCommand(byte command) {
			return ((command & 0x03) == JournalCommand.TABLE_ADD);
		}

		/// <summary>
		/// Returns true if the given command is a removal command.
		/// </summary>
		/// <param name="command"></param>
		/// <returns></returns>
		internal static bool IsRemoveCommand(byte command) {
			return ((command & 0x03) == JournalCommand.TABLE_REMOVE);
		}

		/// <summary>
		/// Adds a command to the journal.
		/// </summary>
		/// <param name="command"></param>
		private void AddCommand(byte command) {
			if (journal_entries >= command_journal.Length) {
				// Resize command array.
				int grow_size = System.Math.Min(4000, journal_entries);
				grow_size = System.Math.Max(4, grow_size);
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
		/// Removes the top n entries from the journal.
		/// </summary>
		/// <param name="n"></param>
		private void RemoveTopEntries(int n) {
			journal_entries = journal_entries - n;
			command_parameters.Crop(0, command_parameters.Count - n);
		}

		/// <summary>
		/// Adds a new command to this journal.
		/// </summary>
		/// <param name="command"></param>
		/// <param name="row_index"></param>
		internal void AddEntry(byte command, int row_index) {
			AddCommand(command);
			AddParameter(row_index);
		}

		// ---------- Getters ----------
		// These methods assume the journal has been setup and no more entries
		// will be made.

		/// <summary>
		/// Gets or sets the commit id for the journal.
		/// </summary>
		/// <remarks>
		/// The commit id is only setted when this change becomes a committed 
		/// change to the database.
		/// </remarks>
		internal long CommitId {
			get { return commit_id; }
			set { commit_id = value; }
		}

		/// <summary>
		/// Gets the table id of the master table this journal is for.
		/// </summary>
		internal int TableId {
			get { return table_id; }
		}

		/// <summary>
		/// Gets the number of entries in the journal.
		/// </summary>
		internal int EntriesCount {
			get { return journal_entries; }
		}

		/// <summary>
		/// Gets the entry command at the given index.
		/// </summary>
		/// <param name="n">The index of the entry to get the command.</param>
		/// <returns>
		/// Return a byte value for a command of an entry at the given index.
		/// </returns>
		internal byte GetCommand(int n) {
			return command_journal[n];
		}

		/// <summary>
		/// Gets the row index of the entry at the given index.
		/// </summary>
		/// <param name="n">The index of the entry to get the row index.</param>
		/// <returns>
		/// Returns the integer value indicating the row index of the entry
		/// at the given index.
		/// </returns>
		internal int GetRowIndex(int n) {
			return command_parameters[n];
		}

		/// <summary>
		/// Gets a normalized array of rows added in the journal.
		/// </summary>
		/// <remarks>
		/// The list won't include the rows signed as removed.
		/// For example, if rows 1, 2, and 3 were added and 2 was 
		/// removed, this will return a list of 1 and 3.
		/// </remarks>
		/// <returns>
		/// Returns an array of integers of all the rows added in the journal.
		/// </returns>
		internal int[] NormalizedAddedRows() {
			IntegerVector list = new IntegerVector();
			int size = EntriesCount;
			for (int i = 0; i < size; ++i) {
				byte tc = GetCommand(i);
				if (tc == JournalCommand.TABLE_ADD || tc == JournalCommand.TABLE_UPDATE_ADD) {
					int row_index = GetRowIndex(i);
					// If row added, add to list
					list.AddInt(row_index);
				} else if (tc == JournalCommand.TABLE_REMOVE || tc == JournalCommand.TABLE_UPDATE_REMOVE) {
					// If row removed, if the row is already in the list
					// it's removed from the list, otherwise we leave as is.
					int row_index = GetRowIndex(i);
					int found_at = list.IndexOf(row_index);
					if (found_at != -1) {
						list.RemoveIntAt(found_at);
					}
				} else {
					throw new ApplicationException("Unknown command in journal.");
				}
			}

			return list.ToIntArray();
		}

		/// <summary>
		/// Gets a normalized array of rows removed in the journal.
		/// </summary>
		/// <returns>
		/// Returns an array of integers of all the rows removed in the journal.
		/// </returns>
		internal int[] NormalizedRemovedRows() {
			IntegerVector list = new IntegerVector();
			int size = EntriesCount;
			for (int i = 0; i < size; ++i) {
				byte tc = GetCommand(i);
				if (tc == JournalCommand.TABLE_REMOVE || tc == JournalCommand.TABLE_UPDATE_REMOVE) {
					// If removed add to the list.
					int row_index = GetRowIndex(i);
					list.AddInt(row_index);
				}
			}
			return list.ToIntArray();
		}

		/// <summary>
		/// Gets all the modified rows in the journal.
		/// </summary>
		/// <remarks>
		/// All the lists are ordered by the order of the command. The update 
		/// list contains two entries per 'update' (the row that was removed 
		/// and the row that was added with the updated info).
		/// <para>
		/// This method is useful for collecting all modification information 
		/// on the table.
		/// </para>
		/// <para>
		/// The order of the array returned is the following:
		/// <list type="number">
		/// <item>The rows added.</item>
		/// <item>The rows removed.</item>
		/// <item>The rows updated (doubled).</item>
		/// </list>
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an array of three integer lists for all the rows modified
		/// in the journal (added, removed and updated).
		/// </returns>
		internal IntegerVector[] AllChangeInformation() {
			IntegerVector[] lists = new IntegerVector[3];
			for (int i = 0; i < 3; ++i) {
				lists[i] = new IntegerVector();
			}
			int size = EntriesCount;
			for (int i = 0; i < size; ++i) {
				byte tc = GetCommand(i);
				int row_index = GetRowIndex(i);
				if (tc == JournalCommand.TABLE_ADD) {
					lists[0].AddInt(row_index);
				} else if (tc == JournalCommand.TABLE_REMOVE) {
					lists[1].AddInt(row_index);
				} else if (tc == JournalCommand.TABLE_UPDATE_ADD || tc == JournalCommand.TABLE_UPDATE_REMOVE) {
					lists[2].AddInt(row_index);
				} else {
					throw new Exception("Don't understand journal command.");
				}
			}
			return lists;
		}

		/// <summary>
		/// Rolls back a given number of the last entries of this journal.
		/// </summary>
		/// <param name="n">The number of entries to roll back.</param>
		/// <remarks>
		/// This method takes into account the transient nature of rows (all added 
		/// rows in the journal are exclusively referenced by this journal).  
		/// The algorithm works as follows: any rows added are deleted, and 
		/// rows deleted (that weren't added) are removed from the journal.
		/// </remarks>
		internal void RollbackEntries(int n) {
			if (n > journal_entries) {
				throw new Exception(
					"Trying to roll back more journal entries than are in the journal.");
			}

			IntegerVector to_add = new IntegerVector();

			// Find all entries and added new rows to the table
			int size = EntriesCount;
			for (int i = size - n; i < size; ++i) {
				byte tc = GetCommand(i);
				if (tc == JournalCommand.TABLE_ADD || tc == JournalCommand.TABLE_UPDATE_ADD) {
					to_add.AddInt(GetRowIndex(i));
				}
			}

			// Delete the top entries
			RemoveTopEntries(n);
			// Mark all added entries to deleted.
			for (int i = 0; i < to_add.Count; ++i) {
				AddEntry(JournalCommand.TABLE_ADD, to_add[i]);
				AddEntry(JournalCommand.TABLE_REMOVE, to_add[i]);
			}

		}



		// ---------- Testing methods ----------

		/// <summary>
		/// Tests a conflict over the journal during a transaction commit.
		/// </summary>
		/// <param name="tableInfo"></param>
		/// <param name="journal"></param>
		/// <remarks>
		/// It assumes that this journal is the journal that is attempting 
		/// to be compatible with the given journal. A journal clashes when 
		/// they both contain a row that is deleted.
		/// </remarks>
		/// <exception cref="TransactionException">
		/// If it detects a clash between journal entries.
		/// </exception>
		internal void TestCommitClash(DataTableInfo tableInfo, MasterTableJournal journal) {
			// Very nasty search here...
			//    int cost = entries() * journal.entries();
			//    Console.Out.Write(" CLASH COST = " + cost + " ");

			for (int i = 0; i < EntriesCount; ++i) {
				byte tc = GetCommand(i);
				if (IsRemoveCommand(tc)) {   // command - row remove
					int row_index = GetRowIndex(i);
					//        Console.Out.WriteLine("* " + row_index);
					for (int n = 0; n < journal.EntriesCount; ++n) {
						//          Console.Out.Write(" " + journal.GetRowIndex(n));
						if (IsRemoveCommand(journal.GetCommand(n)) &&
							journal.GetRowIndex(n) == row_index) {
							throw new TransactionException(
							   TransactionException.RowRemoveClash,
							   "Concurrent Serializable Transaction Conflict(1): " +
							   "Current row remove clash ( row: " + row_index + ", table: " +
							   tableInfo.TableName + " )");
						}
					}
					//        Console.Out.WriteLine();
				}
			}
		}


		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("[MasterTableJournal] [");
			buf.Append(commit_id);
			buf.Append("] (");
			for (int i = 0; i < EntriesCount; ++i) {
				byte c = GetCommand(i);
				int row_index = GetRowIndex(i);
				buf.Append("(");
				buf.Append(c);
				buf.Append(")");
				buf.Append(row_index);
				buf.Append(" ");
			}
			buf.Append(")");
			return buf.ToString();
		}
	}
}