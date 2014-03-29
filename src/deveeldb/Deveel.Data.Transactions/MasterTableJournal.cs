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
using System.Text;

using Deveel.Data.DbSystem;

using SysMath = System.Math;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// A journal of changes that occured to a table in a data conglomerate during
	/// a transaction.
	/// </summary>
	public sealed class MasterTableJournal : IEnumerable<JournalCommand> {
		/// <summary>
		/// The commit id given to this change when it is committed.
		/// </summary>
		/// <remarks>
		/// This is only set when the journal is a committed change to the database.
		/// </remarks>
		private long commitId;


		/// <summary>
		/// The master table id.
		/// </summary>
		private readonly int tableId;

		/// <summary>
		/// The number of entries in this journal.
		/// </summary>
		private int journalEntries;

		/// <summary>
		/// A byte[] array that represents the set of commands a transaction
		/// performed on this table.
		/// </summary>
		private byte[] commandJournal;

		/// <summary>
		/// An IntegerVector that is filled with parameters from the command journal.
		/// </summary>
		/// <remarks>
		/// For example, a 'TABLE_ADD' journal log will have as parameters the
		/// row_index that was added to this table.
		/// </remarks>
		private readonly List<int> commandParameters;

		internal MasterTableJournal(int tableId) {
			this.tableId = tableId;
			commandJournal = new byte[16];
			commandParameters = new List<int>(32);
		}

		internal MasterTableJournal()
			: this(-1) {
		}

		/// <summary>
		/// Adds a command to the journal.
		/// </summary>
		/// <param name="command"></param>
		private void AddCommand(JournalCommandType command) {
			if (journalEntries >= commandJournal.Length) {
				// Resize command array.
				int growSize = SysMath.Min(4000, journalEntries);
				growSize = SysMath.Max(4, growSize);
				byte[] newCommandJournal = new byte[journalEntries + growSize];
				Array.Copy(commandJournal, 0, newCommandJournal, 0, journalEntries);
				commandJournal = newCommandJournal;
			}

			commandJournal[journalEntries] = (byte)command;
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
		/// Removes the top n entries from the journal.
		/// </summary>
		/// <param name="n"></param>
		private void RemoveTopEntries(int n) {
			journalEntries = journalEntries - n;
			commandParameters.RemoveRange(0, commandParameters.Count - n);
		}

		/// <summary>
		/// Adds a new command to this journal.
		/// </summary>
		/// <param name="command"></param>
		/// <param name="rowIndex"></param>
		internal void AddEntry(JournalCommandType command, int rowIndex) {
			AddCommand(command);
			AddParameter(rowIndex);
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
			get { return commitId; }
			set { commitId = value; }
		}

		/// <summary>
		/// Gets the table id of the master table this journal is for.
		/// </summary>
		internal int TableId {
			get { return tableId; }
		}

		/// <summary>
		/// Gets the number of entries in the journal.
		/// </summary>
		internal int EntriesCount {
			get { return journalEntries; }
		}

		/// <summary>
		/// Gets the entry command at the given index.
		/// </summary>
		/// <param name="n">The index of the entry to get the command.</param>
		/// <returns>
		/// Return a byte value for a command of an entry at the given index.
		/// </returns>
		internal JournalCommandType GetCommand(int n) {
			return (JournalCommandType) commandJournal[n];
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
			return commandParameters[n];
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
			List<int> list = new List<int>();
			int size = EntriesCount;
			for (int i = 0; i < size; ++i) {
				JournalCommandType tc = GetCommand(i);
				if (tc == JournalCommandType.AddRow || 
					tc == JournalCommandType.UpdateAddRow) {
					int rowIndex = GetRowIndex(i);
					// If row added, add to list
					list.Add(rowIndex);
				} else if (tc == JournalCommandType.RemoveRow ||
					tc == JournalCommandType.UpdateRemoveRow) {
					// If row removed, if the row is already in the list
					// it's removed from the list, otherwise we leave as is.
					int rowIndex = GetRowIndex(i);
					int foundAt = list.IndexOf(rowIndex);
					if (foundAt != -1) {
						list.RemoveAt(foundAt);
					}
				} else {
					throw new ApplicationException("Unknown command in journal.");
				}
			}

			return list.ToArray();
		}

		/// <summary>
		/// Gets a normalized array of rows removed in the journal.
		/// </summary>
		/// <returns>
		/// Returns an array of integers of all the rows removed in the journal.
		/// </returns>
		internal int[] NormalizedRemovedRows() {
			List<int> list = new List<int>();
			int size = EntriesCount;
			for (int i = 0; i < size; ++i) {
				JournalCommandType tc = GetCommand(i);
				if (tc == JournalCommandType.RemoveRow || 
					tc == JournalCommandType.UpdateRemoveRow) {
					// If removed add to the list.
					int row_index = GetRowIndex(i);
					list.Add(row_index);
				}
			}
			return list.ToArray();
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
		internal List<int>[] AllChangeInformation() {
			List<int>[] lists = new List<int>[3];
			for (int i = 0; i < 3; ++i) {
				lists[i] = new List<int>();
			}
			int size = EntriesCount;
			for (int i = 0; i < size; ++i) {
				JournalCommandType tc = GetCommand(i);
				int rowIndex = GetRowIndex(i);
				if (tc == JournalCommandType.AddRow) {
					lists[0].Add(rowIndex);
				} else if (tc == JournalCommandType.RemoveRow) {
					lists[1].Add(rowIndex);
				} else if (tc == JournalCommandType.UpdateAddRow ||
				           tc == JournalCommandType.UpdateRemoveRow) {
					lists[2].Add(rowIndex);
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
			if (n > journalEntries)
				throw new Exception("Trying to roll back more journal entries than are in the journal.");

			List<int> toAdd = new List<int>();

			// Find all entries and added new rows to the table
			int size = EntriesCount;
			for (int i = size - n; i < size; ++i) {
				JournalCommandType tc = GetCommand(i);
				if (tc == JournalCommandType.AddRow || 
					tc == JournalCommandType.UpdateAddRow) {
					toAdd.Add(GetRowIndex(i));
				}
			}

			// Delete the top entries
			RemoveTopEntries(n);
			// Mark all added entries to deleted.
			for (int i = 0; i < toAdd.Count; ++i) {
				AddEntry(JournalCommandType.AddRow, toAdd[i]);
				AddEntry(JournalCommandType.RemoveRow, toAdd[i]);
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
			for (int i = 0; i < EntriesCount; ++i) {
				JournalCommandType tc = GetCommand(i);
				if (tc == JournalCommandType.RemoveRow) {   // command - row remove
					int rowIndex = GetRowIndex(i);
					for (int n = 0; n < journal.EntriesCount; ++n) {
						if (journal.GetCommand(n) == JournalCommandType.RemoveRow &&
							journal.GetRowIndex(n) == rowIndex) {
							throw new TransactionException(
							   TransactionException.RowRemoveClash,
							   "Concurrent Serializable Transaction Conflict(1): " +
							   "Current row remove clash ( row: " + rowIndex + ", table: " +
							   tableInfo.TableName + " )");
						}
					}
				}
			}
		}


		public IEnumerator<JournalCommand> GetEnumerator() {
			return new CommandEnumerator(this);
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("[MasterTableJournal] [");
			buf.Append(commitId);
			buf.Append("] (");
			for (int i = 0; i < EntriesCount; ++i) {
				JournalCommandType c = GetCommand(i);
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

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#region CommandEnumerator

		class CommandEnumerator : IEnumerator<JournalCommand> {
			private readonly MasterTableJournal journal;
			private int entryCount;
			private int index;

			public CommandEnumerator(MasterTableJournal journal) {
				this.journal = journal;
				index = -1;
				entryCount = journal.EntriesCount;
			}

			private void AssertConsistent() {
				if (entryCount != journal.EntriesCount)
					throw new InvalidOperationException("The journal has changed: the enumeration is not consistent.");
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				AssertConsistent();
				return ++index < entryCount;
			}

			public void Reset() {
				entryCount = journal.EntriesCount;
				index = -1;
			}

			public JournalCommand Current {
				get {
					AssertConsistent();
					JournalCommandType commandType = journal.GetCommand(index);
					int rowIndex = journal.GetRowIndex(index);
					return new JournalCommand(commandType, -1, rowIndex);

				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}