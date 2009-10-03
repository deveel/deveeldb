// 
//  OpenTransactionList.cs
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
					Debug.Write(DebugLevel.Message, this, "Stacks:");
					for (int n = 0; n < open_transaction_stacks.Count; ++n) {
						Debug.WriteException(DebugLevel.Message, (Exception)open_transaction_stacks[n]);
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