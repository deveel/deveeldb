using System;
using System.Data;
using System.Data.Common;


namespace Deveel.Data.Client {
	public sealed class DeveelDbTransaction : DbTransaction {
		private DeveelDbConnection connection;
		private IsolationLevel isolationLevel;
		private bool finished;

		internal DeveelDbTransaction(DeveelDbConnection connection, IsolationLevel isolationLevel, int commitId) {
			if (connection == null)
				throw new ArgumentNullException("connection");

			this.connection = connection;
			CommitId = commitId;
			this.isolationLevel = isolationLevel;
			connection.Transaction = this;
		}

		private void AssertNotFinished() {
			if (finished)
				throw new InvalidOperationException("The transaction was already finished.");
		}


		public override void Commit() {
			if (connection == null ||
			    (connection.State != ConnectionState.Open &&
			     connection.State != ConnectionState.Executing))
				throw new InvalidOperationException("The underlying connection must be opened.");

			AssertNotFinished();

			try {
				Connection.CommitTransaction(CommitId);
			} finally {
				Connection.Transaction = null;
				finished = true;
			}
		}

		public override void Rollback() {
			if (connection == null ||
			    (connection.State != ConnectionState.Open &&
			     connection.State != ConnectionState.Executing))
				throw new InvalidOperationException("The underlying connection must be opened.");

			AssertNotFinished();

			try {
				Connection.RollbackTransaction(CommitId);
			} finally {
				Connection.Transaction = null;
				finished = true;
			}
		}

		public new DeveelDbConnection Connection {
			get { return connection; }
		}

		protected override DbConnection DbConnection {
			get { return Connection; }
		}

		public override IsolationLevel IsolationLevel {
			get { return isolationLevel; }
		}

		internal int CommitId { get; private set; }

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (!finished)
					Rollback();
			}

			connection = null;

			base.Dispose(disposing);
		}
	}
}
