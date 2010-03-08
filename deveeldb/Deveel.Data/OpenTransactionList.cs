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
using System.Text;

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
		/// True to enable transaction tracking.
		/// </summary>
		private const bool TRACKING = false;

		/// <summary>
		/// The system that this transaction list is part of.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The list of open transactions.
		/// </summary>
		private readonly ArrayList open_transactions;

		/// <summary>
		/// A list of <see cref="Exception"/> objects created when the transaction 
		/// is added to the open transactions list.
		/// </summary>
		private readonly ArrayList open_transaction_stacks;

		/// <summary>
		/// The minimum commit id of the current list.
		/// </summary>
		private long minimum_commit_id;

		/// <summary>
		/// The maximum commit id of the current list.
		/// </summary>
		private long maximum_commit_id;

		internal OpenTransactionList(TransactionSystem system) {
			this.system = system;
			open_transactions = new ArrayList();
			if (TRACKING) {
				open_transaction_stacks = new ArrayList();
			}
			minimum_commit_id = Int64.MaxValue;
			maximum_commit_id = 0;
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
				long current_commit_id = transaction.CommitID;
				if (current_commit_id >= maximum_commit_id) {
					open_transactions.Add(transaction);
					if (TRACKING) {
						open_transaction_stacks.Add(new Exception());
					}
					system.Stats.Increment("OpenTransactionList.count");
					maximum_commit_id = current_commit_id;
				} else {
					throw new ApplicationException(
						"Added a transaction with a lower than maximum commit_id");
				}
			}
		}

		/// <summary>
		/// Removes an open transaction from the list.
		/// </summary>
		/// <param name="transaction"></param>
		internal void RemoveTransaction(Transaction transaction) {
			lock (this) {
				int size = open_transactions.Count;
				int i = open_transactions.IndexOf(transaction);
				if (i == 0) {
					// First in list.
					if (i == size - 1) {
						// And last.
						minimum_commit_id = Int32.MaxValue;
						maximum_commit_id = 0;
					} else {
						minimum_commit_id =
							((Transaction)open_transactions[i + 1]).CommitID;
					}
				} else if (i == open_transactions.Count - 1) {
					// Last in list.
					maximum_commit_id =
						((Transaction)open_transactions[i - 1]).CommitID;
				} else if (i == -1) {
					throw new ApplicationException("Unable to find transaction in the list.");
				}
				open_transactions.RemoveAt(i);
				if (TRACKING) {
					open_transaction_stacks.RemoveAt(i);
				}
				system.Stats.Decrement("OpenTransactionList.count");

				if (TRACKING) {
					system.Debug.Write(DebugLevel.Message, this, "Stacks:");
					for (int n = 0; n < open_transaction_stacks.Count; ++n) {
						system.Debug.WriteException(DebugLevel.Message, (Exception)open_transaction_stacks[n]);
					}
				}

			}
		}

		/// <summary>
		/// Returns the number of transactions that are open on the conglomerate.
		/// </summary>
		internal int Count {
			get {
				lock (this) {
					return open_transactions.Count;
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
		internal long MinimumCommitID(Transaction transaction) {
			lock (this) {
				long minimum_commit_id = Int64.MaxValue;
				if (open_transactions.Count > 0) {
					// If the bottom transaction is this transaction, then go to the
					// next up from the bottom (we don't count this transaction as the
					// minimum commit_id).
					Transaction test_transaction = (Transaction)open_transactions[0];
					if (test_transaction != transaction) {
						minimum_commit_id = test_transaction.CommitID;
					} else if (open_transactions.Count > 1) {
						minimum_commit_id =
							((Transaction)open_transactions[1]).CommitID;
					}
				}

				return minimum_commit_id;

			}
		}

		public override String ToString() {
			lock (this) {
				StringBuilder buf = new StringBuilder();
				buf.Append("[ OpenTransactionList: ");
				for (int i = 0; i < open_transactions.Count; ++i) {
					Transaction t = (Transaction)open_transactions[i];
					buf.Append(t.CommitID);
					buf.Append(", ");
				}
				buf.Append(" ]");
				return buf.ToString();
			}

		}
	}
}