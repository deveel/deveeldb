using System;
using System.Transactions;

namespace Deveel.Data.Client {
	class DeveelDbEnlistment : IEnlistmentNotification {
		private DeveelDbTransaction transaction;

		public DeveelDbEnlistment(DeveelDbConnection connection, Transaction scope) {
			transaction = connection.BeginTransaction();

			Scope = scope;
			Scope.EnlistVolatile(this, EnlistmentOptions.None);
		}

		public Transaction Scope { get; private set; }

		public void Prepare(PreparingEnlistment preparingEnlistment) {
			DeveelDbException error;
			if (!transaction.IsOpen(out error)) {
				preparingEnlistment.ForceRollback(error);
			} else {
				preparingEnlistment.Prepared();
			}
		}

		public void Commit(Enlistment enlistment) {
			var connection = transaction.Connection;

			try {
				transaction.AssertOpen();
				transaction.Commit();
				enlistment.Done();
			} finally {
				Dispose(connection);
			}
		}

		private void Dispose(DeveelDbConnection connection) {
			connection.Dispose();
			transaction = null;
			Scope = null;
		}

		public void Rollback(Enlistment enlistment) {
			var connection = transaction.Connection;

			try {
				transaction.Rollback();
				enlistment.Done();
			} finally {
				Dispose(connection);
			}
		}

		public void InDoubt(Enlistment enlistment) {
			enlistment.Done();
		}
	}
}
