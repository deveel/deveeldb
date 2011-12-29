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
using System.Collections.Generic;

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
		/// The table managed by this object.
		/// </summary>
		private readonly MasterTableDataSource table;

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
		private readonly IList<MasterTableJournal> transactionModList;





		// ---------- Stat keys ----------

		private readonly string journalCountStatKey;
		private long tsMergeCount;
		private long tsMergeSize;

		internal MultiVersionTableIndices(TransactionSystem system, MasterTableDataSource table) {
			this.system = system;
			this.table = table;

			transactionModList = new List<MasterTableJournal>();

			journalCountStatKey = "MultiVersionTableIndices.JournalEntries." + table.TableName;

		}

		/// <summary>
		/// Returns the <see cref="ILogger"/> object used to log debug messages.
		/// </summary>
		public Logger Logger {
			get { return system.Logger; }
		}

		/// <summary>
		/// Gets <b>true</b> if this table has any journal modifications that have not
		/// yet been incorporated into master index.
		/// </summary>
		public bool HasTransactionChangesPending {
			get { return transactionModList.Count > 0; }
		}

		/// <summary>
		/// Updates the master records from the journal logs up to the given
		/// commit id.
		/// </summary>
		/// <param name="commitId">The transaction commit id used as minimum
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
		public bool MergeJournalChanges(long commitId) {
			// Average size of pending transactions when this method is called...
			++tsMergeCount;
			tsMergeSize += transactionModList.Count;
			if ((tsMergeCount%32) == 0) {
				system.Stats.Set(
					(int) ((tsMergeSize*1000000L)/tsMergeCount),
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
			while (transactionModList.Count > 0) {
				MasterTableJournal journal = transactionModList[0];

				if (commitId > journal.CommitId) {
					++merge_count;
					if (Logger.IsInterestedIn(LogLevel.Information))
						Logger.Info(this, "Merging '" + table.TableName + "' journal: " + journal);

					// Remove the top journal entry from the list.
					transactionModList.RemoveAt(0);
					system.Stats.Decrement(journalCountStatKey);
				} else {
					// If (commit_id <= journal.CommitId)
					return false;
				}
			}

			return true;
		}

		/// <summary>
		/// Gets all the journals successfully committed after the given
		/// commit id.
		/// </summary>
		/// <param name="commitId">The transaction commit id used as the minimum
		/// commit id for the recovering.</param>
		/// <remarks>
		/// This is part of the conglomerate commit check phase and will be on a
		/// <i>commit lock</i>.
		/// </remarks>
		/// <returns>
		/// Returns a list of all the <see cref="MasterTableJournal"/> that 
		/// have been successfully committed against the underlying table 
		/// having a <see cref="MasterTableJournal.CommitId"/> greater or 
		/// equal to the given <paramref name="commitId"/>.
		/// </returns>
		public MasterTableJournal[] FindAllJournalsSince(long commitId) {
			List<MasterTableJournal> allSince = new List<MasterTableJournal>();
			foreach (MasterTableJournal journal in transactionModList) {
				long journalCommitId = journal.CommitId;
				// All journals that are greater or equal to the given commit id
				if (journalCommitId >= commitId)
					allSince.Add(journal);
			}

			return allSince.ToArray();
		}

		/// <summary>
		/// Adds a transaction journal to the list of modifications on the indices
		/// kept here.
		/// </summary>
		/// <param name="change"></param>
		public void AddTransactionJournal(MasterTableJournal change) {
			transactionModList.Add(change);
			system.Stats.Increment(journalCountStatKey);
		}
	}
}