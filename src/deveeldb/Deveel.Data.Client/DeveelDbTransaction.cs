using System;
using System.Data;
using System.Data.Common;


namespace Deveel.Data.Client {
	public sealed class DeveelDbTransaction : DbTransaction {
		private DeveelDbConnection connection;
		private int commitId;
		private IsolationLevel isolationLevel;

		internal DeveelDbTransaction(DeveelDbConnection connection, IsolationLevel isolationLevel, int commitId) {
			if (connection == null)
				throw new ArgumentNullException("connection");

			this.connection = connection;
			this.commitId = commitId;
			this.isolationLevel = isolationLevel;
		}


		public override void Commit() {
			try {
				Connection.CommitTransaction(commitId);
			} finally {
				Connection.Transaction = null;
			}
		}

		public override void Rollback() {
			try {
				Connection.RollbackTransaction(commitId);
			} finally {
				Connection.Transaction = null;
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
	}
}
