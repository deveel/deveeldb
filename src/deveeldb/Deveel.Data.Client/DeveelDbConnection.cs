using System;
using System.Data;
using System.Data.Common;

namespace Deveel.Data.Client {
	// TODO:
	public sealed class DeveelDbConnection : DbConnection {
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) {
			throw new NotImplementedException();
		}

		public override void Close() {
			throw new NotImplementedException();
		}

		public override void ChangeDatabase(string databaseName) {
			throw new NotImplementedException();
		}

		public override void Open() {
			throw new NotImplementedException();
		}

		public override string ConnectionString { get; set; }

		public override string Database {
			get { throw new NotImplementedException(); }
		}

		public override ConnectionState State {
			get { throw new NotImplementedException(); }
		}

		public override string DataSource {
			get { throw new NotImplementedException(); }
		}

		public override string ServerVersion {
			get { throw new NotImplementedException(); }
		}

		internal DeveelDbTransaction Transaction { get; set; }

		protected override DbCommand CreateDbCommand() {
			throw new NotImplementedException();
		}

		internal void CommitTransaction(int commitId) {
			throw new NotImplementedException();
		}

		internal void RollbackTransaction(int commitId) {
			throw new NotImplementedException();
		}
	}
}
