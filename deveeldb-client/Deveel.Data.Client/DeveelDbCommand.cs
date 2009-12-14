// 
//  DeveelDbCommand.cs
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
	public sealed class DeveelDbCommand : DbCommand {
		public DeveelDbCommand() {
			parameters = new DeveelDbParameterCollection();
		}

		private bool designTimeVisible;
		private DeveelDbConnection connection;
		private DeveelDbTransaction transaction;
		private int commandTimeout;
		private bool timeoutWasSet;
		private string commandText;
		private DeveelDbParameterCollection parameters;

		public override void Prepare() {
			throw new NotImplementedException();
		}

		public override string CommandText {
			get { return commandText; }
			set { commandText = value; }
		}

		public override int CommandTimeout {
			get {
				if (timeoutWasSet)
					return commandTimeout;
				if (connection != null)
					return connection.Settings.QueryTimeout;
				return -1;
			}
			set {
				if (value < 0) {
					timeoutWasSet = false;
					commandTimeout = -1;
				} else {
					commandTimeout = value;
					timeoutWasSet = true;
				}
			}
		}

		public override CommandType CommandType {
			get { return CommandType.Text; }
			set {
				if (value != CommandType.Text)
					throw new NotSupportedException();	// yet...
			}
		}

		public override UpdateRowSource UpdatedRowSource {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		protected override DbConnection DbConnection {
			get { return Connection; }
			set { Connection = (DeveelDbConnection) value; }
		}

		public new DeveelDbConnection Connection {
			get { return connection; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				if (connection != value)
					Transaction = null;

				connection = value;
				transaction = connection.currentTransaction;
			}
		}

		protected override DbParameterCollection DbParameterCollection {
			get { return Parameters; }
		}

		public new DeveelDbParameterCollection Parameters {
			get { return parameters; }
		}

		protected override DbTransaction DbTransaction {
			get { return Transaction; }
			set { Transaction = (DeveelDbTransaction) value; }
		}

		public new DeveelDbTransaction Transaction {
			get { return transaction; }
			set {
				if (value == null && transaction != null)
					transaction = null;
				else if (transaction != null &&
					(value != null && value.Id != transaction.Id))
					throw new ArgumentException();

				transaction = value;
			}
		}

		public override bool DesignTimeVisible {
			get { return designTimeVisible; }
			set { designTimeVisible = value; }
		}

		public override void Cancel() {
			throw new NotImplementedException();
		}

		protected override DbParameter CreateDbParameter() {
			throw new NotImplementedException();
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			if (behavior != CommandBehavior.Default)
				throw new NotSupportedException();	// yet...

			return ExecuteReader();
		}

		public new DeveelDbDataReader ExecuteReader() {
			throw new NotImplementedException();
		}

		public override int ExecuteNonQuery() {
			throw new NotImplementedException();
		}

		public override object ExecuteScalar() {
			throw new NotImplementedException();
		}
	}
}