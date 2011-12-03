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
using System.Data;
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbTransaction : DbTransaction {
		internal DeveelDbTransaction(DeveelDbConnection conn, int id, bool autoCommit) {
			this.id = id;
			this.conn = conn;
			this.autoCommit = autoCommit;
		}

		private readonly int id;
	    private bool committed;
	    private bool rolledback;
		private readonly DeveelDbConnection conn;
		private readonly bool autoCommit;

		internal int Id {
			get { return id; }
		}


		protected override DbConnection DbConnection {
			get { return Connection; }
		}

		#region Implementation of IDisposable

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if ((conn != null && conn.State == ConnectionState.Open) && 
					!committed && !rolledback)
					Rollback();
			}

			base.Dispose(disposing);
		}

		#endregion

		#region Implementation of IDbTransaction

		public override void Commit() {
			if (conn == null || conn.State != ConnectionState.Open)
				throw new InvalidOperationException("The underlying connection must be opened.");

			if (committed)
				throw new InvalidOperationException("The transaction was already committed.");

			try {
				IDbCommand result = conn.CreateCommand("COMMIT");
				result.ExecuteNonQuery();

				// orphans the current transaction in the database...
				conn.currentTransaction = null;
				if (autoCommit)
					conn.AutoCommit = true;
			} finally {
				committed = true;
			}
		}

		public override void Rollback() {
			if (conn == null || conn.State != ConnectionState.Open)
				throw new InvalidOperationException("The underlying connection must be opened.");

			if (rolledback)
				throw new InvalidOperationException("The transaction was already rolledback.");

			try {
				IDbCommand command = conn.CreateCommand("ROLLBACK");
				command.ExecuteNonQuery();

				// orphans the current transaction in the database...
				conn.currentTransaction = null;
				if (autoCommit)
					conn.AutoCommit = true;
			} finally {
				rolledback = true;
			}
		}

	    public new DeveelDbConnection Connection {
            get { return conn; }
	    }

		public override IsolationLevel IsolationLevel {
			get { return IsolationLevel.Serializable; }
		}

		#endregion
	}
}