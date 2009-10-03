// 
//  MultiVersionTableIndices.cs
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

using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// This class manages a set of indices for a table over different versions.
	/// </summary>
	/// <remarks>
	/// The indices include the list of rows (required), and any index data
	/// (optional). 
	/// This object manages table indexes at multiple revision levels.
	/// When indexes are requested, what is returned is an isolated version of the
	/// current indexes. Index changes automatically create a new version and
	/// each version of the index found is isolated from any concurrent changes.
	/// <para>
	/// This class is not thread safe, but it assumes thread safety by the
	/// callee. It is not safe for multi-threaded access.
	/// </para>
	/// </remarks>
	sealed class MultiVersionTableIndices {
		/// <summary>
		/// The name of the table.
		/// </summary>
		private readonly TableName table_name;

		/// <summary>
		/// The number of columns in the referenced table.
		/// </summary>
		private readonly int column_count;

		/// <summary>
		/// The system object.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// A list of MasterTableJournal objects that represent the changes
		/// that have occurred to this master index after various transactions
		/// have been committed.
		/// </summary>
		/// <remarks>
		/// This list can be used to build the indices and a table row enumerator for
		/// snapshots of the table at various transaction check points.
		/// </remarks>
		private readonly ArrayList transaction_mod_list;





		// ---------- Stat keys ----------

		private readonly String journal_count_stat_key;

		internal MultiVersionTableIndices(TransactionSystem system,
								 TableName table_name, int column_count) {
			this.system = system;
			this.table_name = table_name;
			this.column_count = column_count;

			transaction_mod_list = new ArrayList();

			journal_count_stat_key = "MultiVersionTableIndices.journal_entries." +
																			table_name;

		}

		private long TS_merge_count;
		private long TS_merge_size;

		/// <summary>
		/// Returns the <see cref="IDebugLogger"/> object used to log debug messages.
		/// </summary>
		/*
		TODO:
		public IDebugLogger Debug {
			get { return system.Debug; }
		}
		*/

		/// <summary>
		/// Updates the master records from the journal logs up to the given
		/// commit id.
		/// </summary>
		/// <param name="commit_id">The transaction commit id used as minimum
		/// commit id for the merge.</param>
		/// <remarks>
		/// This could be a fairly expensive operation if there are
		/// a lot of modifications because each change could require a lookup
		/// of records in the data source.
		/// <para>
		/// <b>Note:</b> It's extremely important that when this is called, there are no
		/// transactions open that are using the merged journal. If there is, then
		/// a transaction may be able to see changes in a table that were made
		/// after the transaction started.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if all the journals changes have been merged
		/// successfully, otherwise <b>false</b>.
		/// </returns>
		internal bool MergeJournalChanges(long commit_id) {

			// Average size of pending transactions when this method is called...
			++TS_merge_count;
			TS_merge_size += transaction_mod_list.Count;
			if ((TS_merge_count % 32) == 0) {
				system.Stats.Set(
					(int)((TS_merge_size * 1000000L) / TS_merge_count),
					"MultiVersionTableIndices.average_journal_merge_mul_1000000");
				//      DatabaseSystem.stats().set(
				//          TS_merge_size / TS_merge_count,
				//          "MultiVersionTableIndices.average_journal_merge");
				//      DatabaseSystem.stats().set(
				//          TS_merge_size,
				//          "MultiVersionTableIndices.TS_merge_size");
				//      DatabaseSystem.stats().set(
				//          TS_merge_count,
				//          "MultiVersionTableIndices.TS_merge_count");
			}

			int merge_count = 0;
			int size = transaction_mod_list.Count;
			while (transaction_mod_list.Count > 0) {

				MasterTableJournal journal =
									   (MasterTableJournal)transaction_mod_list[0];

				if (commit_id > journal.CommitId) {

					++merge_count;
					if (Debug.IsInterestedIn(DebugLevel.Information)) {
						Debug.Write(DebugLevel.Information, this,
									"Merging '" + table_name + "' journal: " + journal);
					}

					// Remove the top journal entry from the list.
					transaction_mod_list.RemoveAt(0);
					system.Stats.Decrement(journal_count_stat_key);

				} else { // If (commit_id <= journal.getCommitID())
					return false;
				}
			}

			return true;

		}

		/// <summary>
		/// Gets all the journals successfully committed after the given
		/// commit id.
		/// </summary>
		/// <param name="commit_id">The transaction commit id used as the minimum
		/// commit id for the recovering.</param>
		/// <remarks>
		/// This is part of the conglomerate commit check phase and will be on a
		/// <i>commit lock</i>.
		/// </remarks>
		/// <returns>
		/// Returns a list of all the <see cref="MasterTableJournal"/> that 
		/// have been successfully committed against the underlying table 
		/// having a <see cref="MasterTableJournal.CommitId"/> greater or 
		/// equal to the given <paramref name="commit_id"/>.
		/// </returns>
		internal MasterTableJournal[] FindAllJournalsSince(long commit_id) {

			ArrayList all_since = new ArrayList();

			int size = transaction_mod_list.Count;
			for (int i = 0; i < size; ++i) {
				MasterTableJournal journal =
									   (MasterTableJournal)transaction_mod_list[i];
				long journal_commit_id = journal.CommitId;
				// All journals that are greater or equal to the given commit id
				if (journal_commit_id >= commit_id) {
					all_since.Add(journal);
				}
			}

			return (MasterTableJournal[])all_since.ToArray(typeof(MasterTableJournal));
		}

		/// <summary>
		/// Adds a transaction journal to the list of modifications on the indices
		/// kept here.
		/// </summary>
		/// <param name="change"></param>
		internal void AddTransactionJournal(MasterTableJournal change) {
			transaction_mod_list.Add(change);
			system.Stats.Increment(journal_count_stat_key);
		}

		/// <summary>
		/// Gets <b>true</b> if this table has any journal modifications that have not
		/// yet been incorporated into master index.
		/// </summary>
		internal bool HasTransactionChangesPending {
			get {
				//    Console.Out.WriteLine(transaction_mod_list);
				return transaction_mod_list.Count > 0;
			}
		}

		/// <summary>
		/// Returns a string describing the transactions pending on this table.
		/// </summary>
		internal string TransactionChangeString {
			get { return transaction_mod_list.ToString(); }
		}
	}
}