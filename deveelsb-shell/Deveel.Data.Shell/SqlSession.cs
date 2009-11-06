using System;
using System.Collections;
using System.Data;
using System.IO;

using Deveel.Commands;
using Deveel.Data.Client;
using Deveel.Shell;

namespace Deveel.Data.Shell {
	public class SqlSession : IInterruptable, IPropertyHandler {
		private DeveelDBShell app;
		private string name;
		private DateTime connectTime;
		private long statementCount;
		private readonly ConnectionString connectionString;
		private DeveelDbConnection conn;
		private Database metaData;
		private bool auto_commit;
		private bool auto_commit_was_set;
		private bool connected;

		private NameCompleter sessionTables;
		private NameCompleter sessionColumns;

		private readonly PropertyRegistry propertyRegistry;
		private volatile bool interrupted;

		internal SqlSession(DeveelDBShell app, string connectionString) {
			this.app = app;
			this.connectionString = new ConnectionString(connectionString);
			statementCount = 0;
			conn = null;
			propertyRegistry = new PropertyRegistry(this);

			if (Connect()) {
				try {
					SetAutoCommit(false);
				} catch (DataException) {
				}
			}

			propertyRegistry.RegisterProperty("auto-commit", new AutoCommitProperty(this));
		}

		internal SqlSession(DeveelDBShell app, DeveelDbConnection conn) {
			this.app = app;
			connectionString = new ConnectionString(conn.ConnectionString);
			this.conn = conn;
			statementCount = 0;
			propertyRegistry = new PropertyRegistry(this);

			SetAutoCommit(false);

			propertyRegistry.RegisterProperty("auto-commit", new AutoCommitProperty(this));
		}

		public PropertyRegistry Properties {
			get { return propertyRegistry; }
		}

		public ConnectionString ConnectionString {
			get { return connectionString; }
		}

		public string Name {
			get { return name; }
		}

		public string DatabaseInfo {
			get { return conn.Database; }
		}

		public bool IsConnected {
			get { return connected; }
		}

		public Table GetTable(String tableName) {
			return metaData.GetTable(tableName);
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

		internal void SetName(string value) {
			name = value;
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

		public bool PrintMessages {
			get { return !(app.Dispatcher.IsInBatch); }
		}

		public void Write(String s) {
			if (PrintMessages) 
				OutputDevice.Message.Write(s);
		}

		public void WriteLine(String msg) {
			if (PrintMessages) 
				OutputDevice.Message.WriteLine(msg);
		}

		public bool Connect() {
			// close old connection ..
			if (conn != null) {
				try {
					conn.Close();
				} catch (Exception) {
					/* ignore */
				}
				conn = null;
			}

			if (connectionString.UserName == null)
				PromptUserName();

			if (connectionString.Password == null)
				PromptPassword();

			try {
				conn = new DeveelDbConnection(connectionString.ToString());
				conn.Open();
			} catch (DataException e) {
				OutputDevice.Message.WriteLine(e.Message);
				return false;
			}

			if (connectionString.UserName == null) {
				try {
					System.Data.DataTable table = conn.GetSchema("User");
					connectionString.UserName = table.Rows[0]["Name"].ToString();
				} catch (Exception e) {
					OutputDevice.Message.WriteLine(e.Message);
					return false;
				}
			}

			//metaData = new Database(this);
			connectTime = DateTime.Now;
			connected = true;
			return true;
		}

		private void PromptUserName() {
			interrupted = false;
			try {
				SignalInterruptHandler.Current.Push(this);
				connectionString.UserName = Readline.ReadLine("Username: ");
				if (interrupted) {
					throw new IOException("Connect interrupted ..");
				}
			} finally {
				SignalInterruptHandler.Current.Pop();
			}
		}

		private void PromptPassword() {
			interrupted = false;
			try {
				SignalInterruptHandler.Current.Push(this);
				connectionString.Password = Readline.ReadPassword("Password: ");
				if (interrupted)
					throw new IOException("Connect interrupted ..");
			} finally {
				SignalInterruptHandler.Current.Pop();
			}
		}


		// -- Interruptable interface
		public void Interrupt() {
			interrupted = true;
			OutputDevice.Message.AttributeBold();
			OutputDevice.Message.WriteLine(" interrupted; press [RETURN]");
			OutputDevice.Message.AttributeReset();
		}

		public String UserName {
			get { return connectionString.UserName; }
		}

		public TimeSpan Uptime {
			get { return DateTime.Now - connectTime; }
		}
		public long StatementCount {
			get { return statementCount; }
		}

		public void Close() {
			try {
				Connection.Close();
				conn = null;
			} catch (Exception e) {
				OutputDevice.Message.WriteLine(e.Message); // don't care
			}
		}

		/**
		 * returns the current connection of this session.
		 */
		public DeveelDbConnection Connection {
			get { return conn; }
		}

		public NameCompleter TableCompleter {
			get { return sessionTables; }
		}

		public NameCompleter AllColumnsCompleter {
			get {
				if (sessionColumns == null) {
					// This may be a lengthy process..
					interrupted = false;
					SignalInterruptHandler.Current.Push(this);
					NameCompleter tables = TableCompleter;
					if (tables == null)
						return null;
					IEnumerator table = tables.GetNames();
					sessionColumns = new NameCompleter();
					while (!interrupted && table.MoveNext()) {
						string tabName = (string)table.Current;
						ICollection columns = ColumnsFor(tabName);
						IEnumerator cit = columns.GetEnumerator();
						while (cit.MoveNext()) {
							String col = (String)cit.Current;
							sessionColumns.AddName(col);
						}
					}
					if (interrupted)
						sessionColumns = null;

					SignalInterruptHandler.Current.Pop();
				}

				return sessionColumns;
			}
		}

		public IDbCommand CreateCommand() {
			IDbCommand result = null;
			int retries = 2;
			try {
				if (conn.State == ConnectionState.Closed) {
					OutputDevice.Message.WriteLine("connection is closed; reconnect.");
					Connect();
					--retries;
				}
			} catch (Exception) {
				
			}

			while (retries > 0) {
				try {
					result = conn.CreateCommand();
					++statementCount;
					break;
				} catch (Exception) {
					OutputDevice.Message.WriteLine("connection failure. Try to reconnect.");
					try {
						Connect();
					} catch (Exception) {
						
					}
				}
				--retries;
			}
			return result;
		}

		/**
	* fixme: add this to the cached values determined by rehash.
	*/
		public ICollection ColumnsFor(String tabName) {
			ArrayList result = new ArrayList();

			String schema = null;
			int schemaDelim = tabName.IndexOf('.');
			if (schemaDelim > 0) {
				schema = tabName.Substring(0, schemaDelim);
				tabName = tabName.Substring(schemaDelim + 1);
			}
			try {
				System.Data.DataTable columnsTable = Connection.GetSchema(DeveelDbMetadataSchemaNames.Columns,
				                                                          new string[] {null, schema, tabName, null});
				for (int i = 0; i < columnsTable.Rows.Count; i++) {
					DataRow row = columnsTable.Rows[i];
					result.Add(row["COLUMN_NAME"].ToString());
				}
			} catch (Exception e) {
				// ignore.
			}
			return result;
		}

		public void RehashTableCompleter() {
			sessionTables = new NameCompleter();

			try {
				//TODO: check this...
				System.Data.DataTable tablesTable = Connection.GetSchema(DeveelDbMetadataSchemaNames.Tables,
				                                                         new string[] {null, null, null, "TABLE", "VIEW"});
				for (int i = 0; i < tablesTable.Rows.Count; i++) {
					DataRow row = tablesTable.Rows[i];
					sessionTables.AddName(row["TABLE_NAME"].ToString());
				}
			} catch (Exception e) {
				// ignore.
			}
			sessionColumns = null;
		}

		/* ------- Session Properties ----------------------------------- */

		private class AutoCommitProperty : BooleanPropertyHolder {
			internal AutoCommitProperty(SqlSession session)
				: base(false) {
				Value= "off"; // 'off' sounds better in this context.
				this.session = session;
			}

			private SqlSession session;

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

		public IEnumerator CompleteAllColumns(string word) {
			return sessionColumns == null ? null : sessionColumns.GetAlternatives(word);
		}

		public String CorrectTableName(String tabName) {
			IEnumerator it = CompleteTableName(tabName);
			if (it == null)
				return null;

			bool foundSameLengthMatch = false;
			int count = 0;
			String correctedName = null;
			if (it.MoveNext()) {
				String alternative = (String)it.Current;
				bool sameLength = (alternative != null && alternative.Length == tabName.Length);

				foundSameLengthMatch |= sameLength;
				++count;
				if (correctedName == null || sameLength) {
					correctedName = alternative;
				}
			}
			return (count == 1 || foundSameLengthMatch) ? correctedName : null;
		}

		public IEnumerator CompleteTableName(string partialTable) {
			NameCompleter completer = TableCompleter;
			return completer == null ? null : completer.GetAlternatives(partialTable);
		}

		public void UnhashCompleters() {
			sessionTables = null;
		}
	}
}