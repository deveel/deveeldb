// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Transactions {
	public sealed class TransactionCollection : IEnumerable<ITransaction> {
		private readonly List<ITransaction> transactions;
		private long minCommitId;
		private long maxCommitId;
 
		public TransactionCollection(ITransactionContext transactionContext) {
			if (transactionContext == null)
				throw new ArgumentNullException("transactionContext");

			TransactionContext = transactionContext;

			transactions = new List<ITransaction>();
			minCommitId = Int64.MaxValue;
			maxCommitId = 0;
		}

		public ITransactionContext TransactionContext { get; private set; }

		public int Count {
			get {
				lock (this) {
					return transactions.Count;
				}
			}
		}

		public void AddTransaction(ITransaction transaction) {
			lock (this) {
				long currentCommitId = transaction.CommitId;
				if (currentCommitId < maxCommitId)
					throw new ApplicationException("Added a transaction with a lower than maximum commit id");
				
				transactions.Add(transaction);
				//TODO: SystemContext.Stats.Increment(StatsDefaultKeys.OpenTransactionsCount);
				maxCommitId = currentCommitId;
			}
		}

		public void RemoveTransaction(ITransaction transaction) {
			lock (this) {
				int size = transactions.Count;
				int i = transactions.IndexOf(transaction);
				if (i == 0) {
					// First in list.
					if (i == size - 1) {
						// And last.
						minCommitId = Int32.MaxValue;
						maxCommitId = 0;
					} else {
						minCommitId = transactions[i + 1].CommitId;
					}
				} else if (i == transactions.Count - 1) {
					// Last in list.
					maxCommitId = transactions[i - 1].CommitId;
				} else if (i == -1) {
					throw new ApplicationException("Unable to find transaction in the list.");
				}

				transactions.RemoveAt(i);
				//TODO: SystemContext.Stats.Decrement(StatsDefaultKeys.OpenTransactionsCount);
			}
		}

		public long MinimumCommitId(ITransaction transaction) {
			lock (this) {
				long commitId = Int64.MaxValue;
				if (transactions.Count > 0) {
					// If the bottom transaction is this transaction, then go to the
					// next up from the bottom (we don't count this transaction as the
					// minimum commit_id).
					var testTransaction = transactions[0];
					if (testTransaction != transaction) {
						commitId = testTransaction.CommitId;
					} else if (transactions.Count > 1) {
						commitId = transactions[1].CommitId;
					}
				}

				return commitId;
			}
		}

		public IEnumerator<ITransaction> GetEnumerator() {
			lock (this) {
				return transactions.GetEnumerator();
			}
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public ITransaction FindById(int commitId) {
			lock (this) {
				return transactions.FirstOrDefault(x => x.CommitId == commitId);
			}
		}
	}
}
