// 
//  DeveelDbTransaction.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if ((conn != null && conn.State == ConnectionState.Open) &&
					!committed && !rolledback)
					Rollback();
			}

			base.Dispose(disposing);
		}

		#region Overrides of DbTransaction

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

		protected override DbConnection DbConnection {
			get { return conn; }
		}

		public override IsolationLevel IsolationLevel {
			get { return IsolationLevel.Serializable; }
		}

		#endregion
	}
}