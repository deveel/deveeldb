//  
//  DbTransaction.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;

namespace Deveel.Data.Client {
	public sealed class DbTransaction : IDbTransaction {
		internal DbTransaction(DbConnection conn) {
			this.conn = conn;
		}

	    private bool committed;
	    private bool rolledback;
		private DbConnection conn;

		#region Implementation of IDisposable

		public void Dispose() {
            if (!committed && !rolledback)
                Rollback();
		}

		#endregion

		#region Implementation of IDbTransaction

		public void Commit() {
			ResultSet result = conn.CreateCommand().ExecuteQuery("COMMIT");
			result.Close();
            committed = true;
		}

		public void Rollback() {
			ResultSet result = conn.CreateCommand().ExecuteQuery("ROLLBACK");
			result.Close();
		    rolledback = true;
		}

		IDbConnection IDbTransaction.Connection {
			get { return Connection; }
		}

	    public DbConnection Connection {
            get { return conn; }
	    }

		IsolationLevel IDbTransaction.IsolationLevel {
			get { return IsolationLevel.Serializable; }
		}

		#endregion
	}
}