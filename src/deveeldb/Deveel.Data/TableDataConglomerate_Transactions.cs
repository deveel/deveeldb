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
using System.Collections.Generic;
using System.IO;

using Deveel.Diagnostics;

namespace Deveel.Data {
	public sealed partial class TableDataConglomerate {
		/// <summary>
		/// The list of transactions that are currently open over this conglomerate.
		/// </summary>
		/// <remarks>
		/// This list is ordered from lowest commit_id to highest.  This object is
		/// shared with all the children MasterTableDataSource objects.
		/// </remarks>
		private readonly OpenTransactionList openTransactions;

		/// <summary>
		/// Starts a new transaction.
		/// </summary>
		/// <remarks>
		/// The <see cref="Transaction"/> object returned by this method is 
		/// used to read the contents of the database at the time the transaction 
		/// was started. It is also used if any modifications are required to 
		/// be made.
		/// </remarks>
		/// <returns></returns>
		public Transaction CreateTransaction() {
			List<MasterTableDataSource> thisCommittedTables = new List<MasterTableDataSource>();

			// Don't let a commit happen while we are looking at this.
			lock (CommitLock) {
				long thisCommitId = commitId;
				StateStore.StateResource[] committedTableList = stateStore.GetVisibleList();
				for (int i = 0; i < committedTableList.Length; ++i) {
					thisCommittedTables.Add(GetMasterTable((int)committedTableList[i].table_id));
				}

				// Create a set of IIndexSet for all the tables in this transaction.
				int sz = thisCommittedTables.Count;
				List<IIndexSet> indexInfo = new List<IIndexSet>(sz);
				for (int i = 0; i < sz; ++i) {
					MasterTableDataSource mtable = thisCommittedTables[i];
					indexInfo.Add(mtable.CreateIndexSet());
				}

				// Create the transaction and record it in the open transactions list.
				Transaction t = new Transaction(this, thisCommitId, thisCommittedTables, indexInfo);
				openTransactions.AddTransaction(t);
				return t;
			}
		}

		/// <summary>
		/// This is called to notify the conglomerate that the transaction has
		/// closed.
		/// </summary>
		/// <param name="transaction"></param>
		/// <remarks>
		/// This is always called from either the rollback or commit method
		/// of the transaction object.
		/// <para>
		/// <b>Note</b> This increments 'commit_id' and requires that the 
		/// conglomerate is commit locked.
		/// </para>
		/// </remarks>
		private void CloseTransaction(Transaction transaction) {
			bool lastTransaction;
			// Closing must happen under a commit Lock.
			lock (CommitLock) {
				openTransactions.RemoveTransaction(transaction);
				// Increment the commit id.
				++commitId;
				// Was that the last transaction?
				lastTransaction = openTransactions.Count == 0;
			}

			// If last transaction then schedule a clean up event.
			if (lastTransaction) {
				try {
					CleanUpConglomerate();
				} catch (IOException e) {
					Debug.Write(DebugLevel.Error, this, "Error cleaning up conglomerate");
					Debug.WriteException(DebugLevel.Error, e);
				}
			}

		} 
	}
}