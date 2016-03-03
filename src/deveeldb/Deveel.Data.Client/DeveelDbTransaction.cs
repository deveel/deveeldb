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


		public override void Commit() {
			if (connection == null ||
			    (connection.State != ConnectionState.Open &&
			     connection.State != ConnectionState.Executing))
				throw new InvalidOperationException("The underlying connection must be opened.");

			AssertOpen();

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

			AssertOpen();

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

		internal bool IsOpen(out DeveelDbException error) {
			if (connection == null) {
				error = new DeveelDbException("The transaction is not associated to any connection.");
				return false;
			}

			if (connection.State != ConnectionState.Open) {
				error = new DeveelDbException("The associated connection is not open.");
				return false;
			}

			if (finished) {
				error = new DeveelDbException("The transaction was already finished.");
				return false;
			}

			error = null;
			return true;
		}

		internal void AssertOpen() {
			DeveelDbException error;
			if (!IsOpen(out error))
				throw error;
		}
	}
}
