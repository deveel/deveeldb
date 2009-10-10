using System;
using System.Data;
using System.IO;

using Deveel.Collections;
using Deveel.Data.Client;
using Deveel.Shell;

namespace Deveel.Data.Shell {
	public class SQLSession : IInterruptable, IPropertyHandler {
		private DeveelDBShell app;
		private DateTime _connectTime;
		private long _statementCount;
		private ConnectionString connectionString;
		private IDbConnection _conn;
		private SQLMetaData _metaData;
		private bool auto_commit;
		private bool auto_commit_was_set;

		private readonly PropertyRegistry _propertyRegistry;
		private volatile bool _interrupted;

		/**
		 * creates a new SQL session. Open the database connection, initializes
		 * the readline library
		 */
		internal SQLSession(DeveelDBShell app, string connectionString) {
			this.app = app;
			this.connectionString = new ConnectionString(connectionString);
			_statementCount = 0;
			_conn = null;
			_propertyRegistry = new PropertyRegistry(this);

			connect();

			try {
				SetAutoCommit(false);
			} catch (DataException) {
			}

			_propertyRegistry.RegisterProperty("auto-commit", new AutoCommitProperty(this));
		}

		public PropertyRegistry Properties {
			get { return _propertyRegistry; }
		}

		public ConnectionString ConnectionString {
			get { return connectionString; }
		}

		public SQLMetaData getMetaData(ISortedSet/*<String>*/ tableNames) {
			if (_metaData == null) {
				_metaData = new SQLMetaDataBuilder().getMetaData(this, tableNames);
			}
			return _metaData;
		}

		public Table getTable(String tableName) {
			return new SQLMetaDataBuilder().getTable(this, tableName);
		}

		public bool IsAutoCommit {
			get {
				if (!auto_commit_was_set) {
					IDbCommand command = CreateCommand();
					command.CommandText = "SHOW CONNECTION_INFO WHERE var = 'auto_commit'";
					IDataReader reader = command.ExecuteReader();
					if (reader.Read())
						auto_commit = reader.GetBoolean(1);
					reader.Close();
				}
				return auto_commit;
			}
		}

		public void SetAutoCommit(bool value) {
			// The SQL to WriteByte into auto-commit mode.
			IDbCommand command = CreateCommand();
			command.CommandText = value ? "SET AUTO COMMIT ON" : "SET AUTO COMMIT OFF";
			command.ExecuteNonQuery();
			auto_commit = value;
			auto_commit_was_set = true;
		}

		public void Commit() {
			IDbCommand command = CreateCommand();
			command.CommandText = "COMMIT";
			command.ExecuteNonQuery();
		}

		public void Rollback() {
		  	IDbCommand command = CreateCommand();
		  	command.CommandText = "ROLLBACK";
		  	command.ExecuteNonQuery();
		  }

		public bool printMessages() {
			return !(app.Dispatcher.IsInBatch);
		}

		public void print(String msg) {
			if (printMessages()) 
				OutputDevice.Message.Write(msg);
		}

		public void println(String msg) {
			if (printMessages()) 
				OutputDevice.Message.WriteLine(msg);
		}

		public void connect() {
			/*
			 * close old connection ..
			 */
			if (_conn != null) {
				try {
					_conn.Close();
				} catch (Exception) {
					 /* ignore */
				}
				_conn = null;
			}

			if (connectionString.UserName == null || connectionString.Password == null) {
				try {
					_conn = new DbConnection(connectionString);
				} catch (DataException e) {
					OutputDevice.Message.WriteLine(e.Message);
					promptUserPassword();
				}
			}

			if (_conn == null) {
				_conn = new DbConnection(connectionString);
			}

			if (_conn != null && connectionString.UserName == null) {
				try {
					DatabaseMetaData meta = new DatabaseMetaData(_conn);
					connectionString.UserName = meta.getUserName();
				} catch (Exception e) {
					/* ok .. at least I tried */
				}
			}
			_connectTime = DateTime.Now;
		}

		private void promptUserPassword() {
			OutputDevice.Message.WriteLine("============ authorization required ===");
			_interrupted = false;
			try {
				SignalInterruptHandler.Current.Push(this);
				 connectionString.UserName = Readline.ReadLine("Username: ");
				if (_interrupted) {
					throw new IOException("connect interrupted ..");
				}
				connectionString.Password = Readline.ReadPassword("Password: ");
				if (_interrupted) {
					throw new IOException("connect interrupted ..");
				}
			} finally {
				SignalInterruptHandler.Current.Pop();
			}
		}


		// -- Interruptable interface
		public void Interrupt() {
			_interrupted = true;
			OutputDevice.Message.AttributeBold();
			OutputDevice.Message.WriteLine(" interrupted; press [RETURN]");
			OutputDevice.Message.AttributeReset();
		}

		public String UserName {
			get { return _username; }
		}

		public TimeSpan Uptime {
			get { return DateTime.Now - _connectTime; }
		}
		public long StatementCount {
			get { return _statementCount; }
		}

		public void Close() {
			try {
				Connection.Close();
				_conn = null;
			} catch (Exception e) {
				OutputDevice.Message.WriteLine(e.Message); // don't care
			}
		}

		/**
		 * returns the current connection of this session.
		 */
		public IDbConnection Connection {
			get { return _conn; }
		}

		public IDbCommand CreateCommand() {
			IDbCommand result = null;
			int retries = 2;
			try {
				if (_conn.State == ConnectionState.Closed) {
					OutputDevice.Message.WriteLine("connection is closed; reconnect.");
					connect();
					--retries;
				}
			} catch (Exception e) { /* ign */	}

			while (retries > 0) {
				try {
					result = _conn.CreateCommand();
					++_statementCount;
					break;
				} catch (Exception) {
					OutputDevice.Message.WriteLine("connection failure. Try to reconnect.");
					try { connect(); } catch (Exception e) { /* ign */ }
				}
				--retries;
			}
			return result;
		}

		/* ------- Session Properties ----------------------------------- */

		private class AutoCommitProperty : BooleanPropertyHolder {
			internal AutoCommitProperty(SQLSession session)
				: base(false) {
				Value= "off"; // 'off' sounds better in this context.
				this.session = session;
			}

			private SQLSession session;

			public override void OnBooleanPropertyChanged(bool switchOn) {
				/*
				 * due to a bug in Sybase, we have to close the
				 * transaction first before setting autcommit.
				 * This is probably a save choice to do, since the user asks
				 * for autocommit..
				 */
				if (switchOn) {
					session.Commit();
				}
				session.SetAutoCommit(switchOn);
				if (session.IsAutoCommit != switchOn) {
					throw new Exception("request ignored");
				}
			}

			public override String DefaultValue {
				get { return "off"; }
			}

			public override String ShortDescription {
				get { return "Switches auto commit"; }
			}
		}
	}
}