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
using System.Collections.Generic;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Transactions {
	public abstract class TableCommitCallback {
		private readonly List<int> addedList;
		private readonly List<int> removedList;

		protected TableCommitCallback(ObjectName tableName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;

			addedList = new List<int>();
			removedList = new List<int>();
		}

		public ObjectName TableName { get; private set; }

		protected bool IsInTransaction { get; private set; }

		internal void OnTransactionStarted() {
			IsInTransaction = true;
			Act();
		}

		internal void OnTransactionEnd() {
			IsInTransaction = false;
			Act();
		}

		public void AttachTo(ITransaction transaction) {
			transaction.RegisterOnCommit(OnCommit);

			if (transaction is ICallbackHandler)
				((ICallbackHandler)transaction).OnCallbackAttached(this);
		}

		public void DetachFrom(ITransaction transaction) {
			transaction.UnregisterOnCommit(OnCommit);

			if (transaction is ICallbackHandler)
				((ICallbackHandler)transaction).OnCallbackDetached(this);
		}

		private void Act() {
			IList<int> add, remove;
			lock (removedList) {
				add = new List<int>(addedList);
				remove = new List<int>(removedList);

				addedList.Clear();
				removedList.Clear();
			}

			OnAction(add, remove);
		}

		private void OnCommit(TableCommitInfo commitInfo) {
			if (TableName.Equals(commitInfo.TableName)) {
				addedList.AddRange(commitInfo.AddedRows);
				removedList.AddRange(commitInfo.RemovedRows);
			}
		}

		protected abstract void OnAction(IEnumerable<int> addedRows, IEnumerable<int> removedRows);
	}
}