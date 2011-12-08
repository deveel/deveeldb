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
	/// The list of all currently open transactions.
	/// </summary>
	/// <remarks>
	/// This is a thread safe object that is shared between a 
	/// <see cref="TableDataConglomerate"/> and its children <see cref="MasterTableDataSource"/> 
	/// objects. It is used for maintaining a list of transactions that are 
	/// currently open in the system. It also provides various utility methods 
	/// around the list.
	/// <para>
	/// This class is thread safe and can safely be accessed by multiple threads.
	/// This is so threads accessing table source information as well as
	/// conglomerate <c>COMMIT</c> stages can safely access this object.
	/// </para>
	/// </remarks>
	sealed class OpenTransactionList {
		/// <summary>
		/// The system that this transaction list is part of.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The list of open transactions.
		/// </summary>
		private readonly List<Transaction> openTransactions;

		/// <summary>
		/// A list of <see cref="Exception"/> objects created when the transaction 
		/// is added to the open transactions list.
		/// </summary>
		private readonly List<Exception> openTransactionStacks;

		/// <summary>
		/// The minimum commit id of the current list.
		/// </summary>
		private long minimumCommitId;

		/// <summary>
		/// The maximum commit id of the current list.
		/// </summary>
		private long maximumCommitId;

		internal OpenTransactionList(TransactionSystem system) {
			this.system = system;
			openTransactions = new List<Transaction>();
#if DEBUG
			openTransactionStacks = new List<Exception>();
#endif
			minimumCommitId = Int64.MaxValue;
			maximumCommitId = 0;
		}

		/// <summary>
		/// Adds a new open transaction to the list.
		/// </summary>
		/// <param name="transaction"></param>
		/// <remarks>
		/// Transactions must be added in order of commit_id.
		/// </remarks>
		internal void AddTransaction(Transaction transaction) {
			lock (this) {
				long currentCommitId = transaction.CommitID;
				if (currentCommitId >= maximumCommitId) {
					openTransactions.Add(transaction);
#if DEBUG
					openTransactionStacks.Add(new Exception());
#endif
					system.Stats.Increment("OpenTransactionList.count");
					maximumCommitId = currentCommitId;
				} else {
					throw new ApplicationException("Added a transaction with a lower than maximum commit_id");
				}
			}
		}

		/// <summary>
		/// Removes an open transaction from the list.
		/// </summary>
		/// <param name="transaction"></param>
		internal void RemoveTransaction(Transaction transaction) {
			lock (this) {
				int size = openTransactions.Count;
				int i = openTransactions.IndexOf(transaction);
				if (i == 0) {
					// First in list.
					if (i == size - 1) {
						// And last.
						minimumCommitId = Int32.MaxValue;
						maximumCommitId = 0;
					} else {
						minimumCommitId = openTransactions[i + 1].CommitID;
					}
				} else if (i == openTransactions.Count - 1) {
					// Last in list.
					maximumCommitId = openTransactions[i - 1].CommitID;
				} else if (i == -1) {
					throw new ApplicationException("Unable to find transaction in the list.");
				}

				openTransactions.RemoveAt(i);
#if DEBUG
				openTransactionStacks.RemoveAt(i);
#endif
				system.Stats.Decrement("OpenTransactionList.count");

#if DEBUG
				system.Debug.Write(DebugLevel.Message, this, "Stacks:");
				for (int n = 0; n < openTransactionStacks.Count; ++n) {
					system.Debug.WriteException(DebugLevel.Message, openTransactionStacks[n]);
				}
#endif
			}
		}

		/// <summary>
		/// Returns the number of transactions that are open on the conglomerate.
		/// </summary>
		public int Count {
			get {
				lock (this) {
					return openTransactions.Count;
				}
			}
		}

		/// <summary>
		/// Returns the minimum commit id not including the given transaction 
		/// object.
		/// </summary>
		/// <param name="transaction"></param>
		/// <returns>
		/// Returns <see cref="Int64.MaxValue"/> if there are no open transactions 
		/// in the list(not including the given transaction).
		/// </returns>
		public long MinimumCommitID(Transaction transaction) {
			lock (this) {
				long minimum_commit_id = Int64.MaxValue;
				if (openTransactions.Count > 0) {
					// If the bottom transaction is this transaction, then go to the
					// next up from the bottom (we don't count this transaction as the
					// minimum commit_id).
					Transaction testTransaction = openTransactions[0];
					if (testTransaction != transaction) {
						minimum_commit_id = testTransaction.CommitID;
					} else if (openTransactions.Count > 1) {
						minimum_commit_id = openTransactions[1].CommitID;
					}
				}

				return minimum_commit_id;

			}
		}
	}
}