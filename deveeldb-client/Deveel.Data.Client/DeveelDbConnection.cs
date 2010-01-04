// 
//  DeveelDbConnection.cs
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
using System.ComponentModel;
using System.Data;
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbConnection : DbConnection {
		public DeveelDbConnection(string connectionString)
			: this() {
			ConnectionString = connectionString;
		}

		public DeveelDbConnection() {
			settings = new DeveelDbConnectionStringBuilder();
			rowCache = new RowCache(this, settings.RowCacheSize);
		}

		private bool autoCommit;
		private ConnectionState state;
		internal DeveelDbTransaction currentTransaction;
		private static int transactionCounter = 0;
		private Driver driver;
		private DeveelDbConnectionStringBuilder settings;
		private string database;
		private DatabaseMetadata metadata;
		private RowCache rowCache;

		internal DeveelDbConnectionStringBuilder Settings {
			get { return settings; }
		}

		/// <summary>
		/// Toggles the <c>AUTO COMMIT</c> flag.
		/// </summary>
		public bool AutoCommit {
			get { return autoCommit; }
			set {
				if (autoCommit == value)
					return;

				if (currentTransaction != null)
					throw new InvalidOperationException("A transaction is already opened.");

				// The SQL to write into auto-commit mode.
				string commandText = "SET AUTO COMMIT ";
				if (value) {
					commandText += "ON";
				} else {
					commandText += "OFF";
				}

				CreateCommand(commandText).ExecuteNonQuery();
				autoCommit = value;
			}
		}

		internal RowCache RowCache {
			get { return rowCache; }
		}

		internal Driver Driver {
			get { return driver; }
		}

		internal void SetState(ConnectionState newState, bool raiseEvent) {
			if (newState == state && !raiseEvent)
				return;

			ConnectionState oldState = state;
			state = newState;

			if (raiseEvent)
				OnStateChange(new StateChangeEventArgs(oldState, state));
		}

		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) {
			if (isolationLevel != IsolationLevel.Serializable)
				throw new ArgumentException("Only SERIALIZABLE transactions are supported.");
			return BeginTransaction();
		}

		/// <inheritdoc/>
		public new DeveelDbTransaction BeginTransaction() {
			//TODO: support multiple transactions...
			if (currentTransaction != null)
				throw new InvalidOperationException("A transaction was already opened on this connection.");

			bool initAutoCommit = false;
			if (AutoCommit) {
				AutoCommit = false;
				initAutoCommit = true;
			}

			int id;
			lock (typeof(DeveelDbConnection)) {
				id = transactionCounter++;
			}

			currentTransaction = new DeveelDbTransaction(this, id, initAutoCommit);
			return currentTransaction;
		}

		public override void Close() {
			if (state == ConnectionState.Closed)
				return;

			try {
				if (currentTransaction != null)
					currentTransaction.Rollback();
			} catch(Exception) {
				// silently ignore any error here...
			}

			try {
				driver.Close();
			} catch (Exception) {
				//TODO: log the error...
			} finally {
				driver = null;
			}

			SetState(ConnectionState.Closed, true);
		}

		public override void ChangeDatabase(string databaseName) {
			if (state == ConnectionState.Closed ||
				state == ConnectionState.Broken)
				throw new InvalidOperationException();

			if (database == databaseName)
				return;

			try {
				driver.ChangeDatabase(databaseName);
				settings.Database = databaseName;
				database = databaseName;
			} catch(Exception) {
				throw new DeveelDbException();
			}
		}

		public override void Open() {
			if (state == ConnectionState.Open)
				throw new InvalidOperationException();

			SetState(ConnectionState.Connecting, true);

			try {
				if (IsLocal) {
					driver = Driver.CreateLocal(settings.Path, settings.QueryTimeout);
					if (settings.Create) {
						((EmbeddedDriver) driver).CreateDatabase(settings.Database, settings.UserName, settings.Password);
					} else {
						((EmbeddedDriver) driver).StartDatabase(settings.Database);
					}
				} else {
					driver = Driver.CreateRemote(settings.Host, settings.Port, settings.QueryTimeout);
				}

				driver.Authenticate(settings);

				SetState(ConnectionState.Open, false);
			} catch (DeveelDbException) {
				SetState(ConnectionState.Broken, true);
				throw;
			} catch (Exception) {
				SetState(ConnectionState.Broken, true);
				throw new DeveelDbException();
			}

			//TODO: auto-enlist into System.Transactions...

			metadata = new DatabaseMetadata(this);
			rowCache = new RowCache(this, settings.RowCacheSize);

			SetState(ConnectionState.Open, true);
		}

		public bool Ping() {
			if (driver != null && driver.Ping())
				return true;

			driver = null;
			SetState(ConnectionState.Closed, true);
			return false;
		}

		public override string ConnectionString {
			get { return (settings == null ? String.Empty : settings.ToString()); }
			set {
				if (state != ConnectionState.Closed)
					throw new InvalidOperationException();

				if (value != null && value.Length> 0) {
					settings = new DeveelDbConnectionStringBuilder(value);
					database = settings.Database;
				}
			}
		}

		internal bool IsLocal {
			get { return settings != null && String.Compare(settings.Host, "(local)", true) == 0; }
		}

		public override string Database {
			get { return database; }
		}

		[Browsable(false)]
		public override ConnectionState State {
			get { return state; }
		}

		[Browsable(true)]
		public override string DataSource {
			get { return settings.DataSource; }
		}

		[Browsable(false)]
		public override string ServerVersion {
			get { return driver == null ? String.Empty : driver.ServerVersion.ToString(); }
		}

		protected override DbCommand CreateDbCommand() {
			return CreateCommand();
		}

		public new DeveelDbCommand CreateCommand() {
			DeveelDbCommand command = new DeveelDbCommand();
			command.Connection = this;
			return command;
		}

		public DeveelDbCommand CreateCommand(string commandText) {
			DeveelDbCommand command = CreateCommand();
			command.CommandText = commandText;
			return command;
		}

		public override DataTable GetSchema(string collectionName, string[] restrictionValues) {
			if (state == ConnectionState.Closed)
				throw new DeveelDbException();

			return metadata.GetSchema(collectionName, restrictionValues);
		}
	}
}