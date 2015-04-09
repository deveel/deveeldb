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
			transaction.Context.Database.TableComposite.RegisterOnCommit(OnCommit);

			if (transaction is ICallbackHandler)
				((ICallbackHandler)transaction).OnCallbackAttached(this);
		}

		public void DetachFrom(ITransaction transaction) {
			transaction.Context.Database.TableComposite.UnregisterOnCommit(OnCommit);

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