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
using System.Collections;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Transactions;

using Deveel.Data.Control;
using Deveel.Data.Protocol;
using Deveel.Data.Server;

using IsolationLevel=System.Data.IsolationLevel;

namespace Deveel.Data.Client {
	///<summary>
	/// Implementation of the <see cref="IDbConnection">connection</see> object 
	/// to a database.
	///</summary>
	/// <remarks>
	/// The implementation specifics for how the connection talks with the database
	/// is left up to the implementation of <see cref="IDatabaseInterface"/>.
	/// <para>
	/// This object is thread safe. It may be accessed safely from concurrent threads.
	/// </para>
	/// </remarks>
	public class DeveelDbConnection : DbConnection, IDatabaseCallBack {
		/// <summary>
		/// The <see cref="DbController"/> in a local connection.
		/// </summary>
		private DbController controller;

		/// <summary>
		/// The mapping of the database configuration URL string to the 
		/// <see cref="ILocalBootable"/> object that manages the connection.
		/// </summary>
		/// <remarks>
		/// This mapping is only used if the driver makes local connections (eg. 'local://').
		/// </remarks>
		private readonly Hashtable local_session_map = new Hashtable();

		/// <summary>
		/// A cache of all rows retrieved from the server.
		/// </summary>
		/// <remarks>
		/// This cuts down the number of requests to the server by caching rows that 
		/// are accessed frequently.  Note that cells are only cached within a ResultSet 
		/// bounds. Two different ResultSet's will not share cells in the cache.
		/// </remarks>
		private RowCache row_cache;

		/// <summary>
		/// The string used to make this connection.
		/// </summary>
		private DeveelDbConnectionStringBuilder connectionString;

		/// <summary>
		/// Set to true if the connection is closed.
		/// </summary>
		private bool is_closed;

		/// <summary>
		/// Set to true if the connection is in auto-commit mode.
		/// (By default, auto_commit is enabled).
		/// </summary>
		private bool auto_commit;

		/// <summary>
		/// The interface to the database.
		/// </summary>
		private IDatabaseInterface db_interface;

		/// <summary>
		/// The list of trigger listeners registered with the connection.
		/// </summary>
		private ArrayList trigger_list;

		/// <summary>
		/// A Thread that handles all dispatching of trigger events to the client.
		/// </summary>
		private TriggerDispatchThread trigger_thread;

		/// <summary>
		/// This is set to true if the ResultSet column lookup methods are case
		/// insensitive.
		/// </summary>
		/// <remarks>
		/// This should be set to true for any database that has case insensitive 
		/// identifiers.
		/// </remarks>
		private bool case_insensitive_identifiers;

		/// <summary>
		/// A mapping from a streamable object id to <see cref="Stream"/> used to 
		/// represent the object when being uploaded to the database engine.
		/// </summary>
		private Hashtable s_object_hold;

		/// <summary>
		/// An unique id count given to streamable object being uploaded to the server.
		/// </summary>
		private long s_object_id;

		/// <summary>
		/// The current state of the connection;
		/// </summary>
		private ConnectionState state;

		/// <summary>
		/// If the user calls the method <see cref="BeginTransaction"/> this field
		/// is set and other calls to the method will trow an exception.
		/// </summary>
		internal DeveelDbTransaction currentTransaction;

		private static int transactionCounter = 0;

		private DatabaseMetadata metadata;


		// For synchronization in this object,
		private readonly Object stateLock = new Object();

		internal DeveelDbConnection(string connectionString, IDatabaseInterface db_interface, int cache_size, int max_size) {
			this.connectionString = new DeveelDbConnectionStringBuilder(connectionString);
			this.db_interface = db_interface;
			is_closed = true;
			auto_commit = true;
			trigger_list = new ArrayList();
			case_insensitive_identifiers = false;
			row_cache = new RowCache(cache_size, max_size);
			s_object_hold = new Hashtable();
			s_object_id = 0;
			state = ConnectionState.Closed;
		}

		/// <summary>
		/// Constructs a <see cref="DeveelDbConnection"/> with a given connection string.
		/// </summary>
		/// <param name="connectionString">The string containing the configuration
		/// to establish a connection with a local or remote database.</param>
		/// <seealso cref="DeveelDbConnection(DeveelDbConnectionStringBuilder)"/>
		public DeveelDbConnection(string connectionString)
			: this(new DeveelDbConnectionStringBuilder(connectionString)) {
		}

		private DeveelDbConnection(DeveelDbConnectionStringBuilder connectionString) {
			this.connectionString = connectionString;
			Init();
		}

		/// <summary>
		/// Constructs an empty <see cref="DeveelDbConnection"/>.
		/// </summary>
		/// <remarks>
		/// This construction of the <see cref="DeveelDbConnection"/> does not
		/// contain any configuration to establish a connection: once created
		/// the object, the <see cref="ConnectionString"/> property must be set.
		/// </remarks>
		public DeveelDbConnection() {
		}

		private void Init() {
			int row_cache_size;
			int max_row_cache_size;

			// If we are to connect to a single user database running
			// within this runtime.
			if (IsLocal(connectionString.Host)) {
				ConnectToLocal();

				// Internal row cache setting are set small.
				row_cache_size = 43;
				max_row_cache_size = 4092000;
			} else {
				try {
					Thread.Sleep(85);
				} catch (ThreadInterruptedException) { /* ignore */ }

				// Make the connection
				db_interface = new TCPStreamDatabaseInterface(connectionString.Host,
				                                              connectionString.Port,
				                                              connectionString.Database);

				// For remote connection, row cache uses more memory.
				row_cache_size = 4111;
				max_row_cache_size = 8192000;

			}

			is_closed = true;
			auto_commit = true;
			trigger_list = new ArrayList();
			case_insensitive_identifiers = false;
			row_cache = new RowCache(row_cache_size, max_row_cache_size);
			s_object_hold = new Hashtable();
			s_object_id = 0;
			state = ConnectionState.Closed;
		}

		/// <summary>
		/// Makes a connection to a local database.
		/// </summary>
		/// <remarks>
		/// If a local database connection has not been made then it is created here.
		/// </remarks>
		/// <returns>
		/// Returns a list of two elements, (<see cref="IDatabaseInterface"/>) db_interface 
		/// and (<see cref="String"/>) database_name.
		/// </returns>
		private void ConnectToLocal() {
			lock (this) {
				// If the ILocalBootable object hasn't been created yet, do so now via
				// reflection.

				// The path to the configuration
				string rootPath = connectionString.Path;
				if (String.IsNullOrEmpty(rootPath))
					rootPath = Environment.CurrentDirectory;

				controller = DbController.Create(rootPath);

				// Is there already a local connection to this database?
				String session_key = rootPath.ToLower();
				ILocalBootable localBootable = (ILocalBootable)local_session_map[session_key];

				// No so create one and WriteByte it in the connection mapping
				if (localBootable == null) {
					localBootable = CreateDefaultLocalBootable(controller, connectionString.Database);
					local_session_map[session_key] = localBootable;
				}

				// Is the connection booted already?
				if (localBootable.IsBooted) {
					// Yes, so simply login.
					db_interface = localBootable.Connect();
				} else {
					// Otherwise we need to boot the local database.

					bool create_db = connectionString.Create;
					bool create_db_if_not_exist = connectionString.BootOrCreate;

					DbConfig config = controller.Config;

					//TODO: set the additional configurations from the connection string
					/*
					string database_path = connectionString.AdditionalProperties["DatabasePath"] as string;
					if (database_path == null)
						database_path = Path.Combine(root_path, connectionString.Database);
					*/

					// Check if the database exists
					bool database_exists = localBootable.CheckExists();

					// If database doesn't exist and we've been told to create it if it
					// doesn't exist, then set the 'create_db' flag.
					if (create_db_if_not_exist && !database_exists) {
						create_db = true;
					}

					// Error conditions;
					// If we are creating but the database already exists.
					if (create_db && database_exists) {
						throw new DataException("Can not create database because a database already exists.");
					}
					// If we are booting but the database doesn't exist.
					if (!create_db && !database_exists) {
						throw new DataException("Can not find a database to start.  Either the database needs to " +
												"be created or the 'database_path' property of the configuration " +
												"must be set to the location of the data files.");
					}

					// Are we creating a new database?
					if (create_db) {
						String username = connectionString.UserName;
						String password = connectionString.Password;

						db_interface = localBootable.Create(username, password, config);
					}
						// Otherwise we must be logging onto a database,
					else {
						db_interface = localBootable.Boot(config);
					}
				}
			}
		}

		/// <summary>
		/// Creates a new <see cref="ILocalBootable"/> object that is used to manage 
		/// the connections to a database running locally.
		/// </summary>
		/// <remarks>
		/// This uses reflection to create a new <see cref="DefaultLocalBootable"/> object. We use 
		/// reflection here because we don't want to make a source level dependency link to the class.
		/// </remarks>
		/// <exception cref="DataException">
		/// If the class <c>DefaultLocalBootable</c> was not found.
		/// </exception>
		private static ILocalBootable CreateDefaultLocalBootable(DbController controller, string databaseName) {
			try {
				Type c = Type.GetType("Deveel.Data.Server.DefaultLocalBootable");
				return (ILocalBootable)Activator.CreateInstance(c, new object[] { controller, databaseName });
			} catch (Exception) {
				// A lot of people ask us about this error so the message is verbose.
				throw new DataException("I was unable to find the class that manages local database " +
				                        "connections.  This means you may not have included the correct " +
				                        "library in your references.");
			}
		}

		private static bool IsLocal(string host) {
			return String.Compare(host, "{local}", true) == 0;
		}

		///<summary>
		/// Toggles whether this connection is handling identifiers as case
		/// insensitive or not. 
		///</summary>
		/// <remarks>
		/// If this is true then <see cref="DeveelDbDataReader.GetString">CreateString("app.id")</see> 
		/// will match against <c>APP.id</c>, etc.
		/// </remarks>
		internal bool IsCaseInsensitiveIdentifiers {
			set { case_insensitive_identifiers = value; }
			get { return case_insensitive_identifiers; }
		}

		/// <summary>
		/// Returns the row Cache object for this connection.
		/// </summary>
		internal RowCache RowCache {
			get { return row_cache; }
		}

		public override string DataSource {
			get { return IsLocal(Settings.Host) ? String.Empty : Settings.Host + ":" + Settings.Port; }
		}

		public override string ServerVersion {
			get { return IsLocal(Settings.Host) ? String.Empty : ((RemoteDatabaseInterface)db_interface).ServerVersion.ToString(2); }
		}


		internal virtual bool InternalOpen() {
			string username = connectionString.UserName;
			string password = connectionString.Password;
			string default_schema = connectionString.Schema;

			if (username == null || username.Equals("") ||
				password == null || password.Equals("")) {
				throw new DataException("username or password have not been set.");
			}

			// Set the default schema to username if it's null
			if (default_schema == null) {
				default_schema = username;
			}

			try {
				if (db_interface is TCPStreamDatabaseInterface)
					// Attempt to open a socket to the database.
					(db_interface as TCPStreamDatabaseInterface).ConnectToDatabase();
			} catch(Exception e) {
				//TODO: log the exception...
				return false;
			}

			// Login with the username/password
			return db_interface.Login(default_schema, username, password, this);
		}

#if !MONO
		public override void EnlistTransaction (System.Transactions.Transaction transaction) {
			if (currentTransaction != null)
				throw new InvalidOperationException ();
	
			if (!transaction.EnlistPromotableSinglePhase (new PromotableConnection (this)))
				throw new InvalidOperationException ();
		}
#endif

		public override void Open() {
			lock (stateLock) {
				if (state != ConnectionState.Closed)
					throw new DataException("Unable to login to connection because it is open.");

				state = ConnectionState.Connecting;
			}

			bool success = InternalOpen();

			lock (stateLock) {
				state = (success ? ConnectionState.Open : ConnectionState.Broken);
			}

			if (success) {
				//TODO: separate from the Open procedure?
				// Determine if this connection is case insensitive or not,
				IsCaseInsensitiveIdentifiers = false;
				IDbCommand stmt = CreateCommand("SHOW CONNECTION_INFO");
				IDataReader rs = stmt.ExecuteReader();
				while (rs.Read()) {
					String key = rs.GetString(0);
					if (key.Equals("case_insensitive_identifiers")) {
						String val = rs.GetString(1);
						IsCaseInsensitiveIdentifiers = val.Equals("true");
					} else if (key.Equals("auto_commit")) {
						String val = rs.GetString(1);
						auto_commit = val.Equals("true");
					}
				}
				rs.Close();
			}
		}

		internal void PushStreamableObjectPart(ReferenceType type, long objectId, long length, byte[] buffer, long offset, int count) {
			db_interface.PushStreamableObjectPart(type, objectId, length, buffer, offset, count);
		}

		/// <summary>
		/// Uploads any streamable objects found in an SqlQuery into the database.
		/// </summary>
		/// <param name="sql"></param>
		private void UploadStreamableObjects(SqlQuery sql) {
			// Push any streamable objects that are present in the Query onto the
			// server.
			Object[] vars = sql.Variables;
			try {
				for (int i = 0; i < vars.Length; ++i) {
					// For each streamable object.
					if (vars[i] != null && vars[i] is StreamableObject) {
						// Buffer size is fixed to 64 KB
						const int BUF_SIZE = 64 * 1024;

						StreamableObject s_object = (StreamableObject)vars[i];
						long offset = 0;
						ReferenceType type = s_object.Type;
						long total_len = s_object.Size;
						long id = s_object.Identifier;
						byte[] buf = new byte[BUF_SIZE];

						// Get the InputStream from the StreamableObject hold
						Object sob_id = id;
						Stream i_stream = (Stream)s_object_hold[sob_id];
						if (i_stream == null) {
							throw new Exception("Assertion failed: Streamable object Stream is not available.");
						}

						i_stream.Seek(0, SeekOrigin.Begin);

						while (offset < total_len) {
							// Fill the buffer
							int index = 0;
							int block_read = (int)System.Math.Min((long)BUF_SIZE, (total_len - offset));
							int to_read = block_read;
							while (to_read > 0) {
								int count = i_stream.Read(buf, index, to_read);
								if (count == -1) {
									throw new IOException("Premature end of stream.");
								}
								index += count;
								to_read -= count;
							}

							// Send the part of the streamable object to the database.
							db_interface.PushStreamableObjectPart(type, id, total_len, buf, offset, block_read);
							// Increment the offset and upload the next part of the object.
							offset += block_read;
						}

						// Remove the streamable object once it has been written
						s_object_hold.Remove(sob_id);

						//        [ Don't close the input stream - we may only want to WriteByte a part of
						//          the stream into the database and keep the file open. ]
						//          // Close the input stream
						//          i_stream.close();

					}
				}
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				throw new DataException("IO Error pushing large object to server: " +
										e.Message);
			}
		}

		/// <summary>
		/// Sends the batch of SqlQuery objects to the database to be executed.
		/// </summary>
		/// <param name="_queries"></param>
		/// <param name="results">The consumer objects for the Query results.</param>
		/// <remarks>
		/// If a Query succeeds then we are guarenteed to know that size of the result set.
		/// <para>
		/// This method blocks until all of the _commands have been processed by the database.
		/// </para>
		/// </remarks>
		internal void ExecuteQueries(SqlQuery[] _queries, ResultSet[] results) {
			// For each Query
			for (int i = 0; i < _queries.Length; ++i) {
				ExecuteQuery(_queries[i], results[i]);
			}
		}

		/// <summary>
		/// Sends the SQL string to the database to be executed.
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="result_set">The consumer for the results from the database.</param>
		/// <remarks>
		/// We are guarenteed that if the Query succeeds that we know the size of the 
		/// result set and at least first first row of the set.
		/// <para>
		/// This method will block until we have received the result header information.
		/// </para>
		/// </remarks>
		internal void ExecuteQuery(SqlQuery sql, ResultSet result_set) {
			UploadStreamableObjects(sql);
			// Execute the Query,
			IQueryResponse resp = db_interface.ExecuteQuery(sql);

			// The format of the result
			ColumnDescription[] col_list = new ColumnDescription[resp.ColumnCount];
			for (int i = 0; i < col_list.Length; ++i) {
				col_list[i] = resp.GetColumnDescription(i);
			}
			// Set up the result set to the result format and update the time taken to
			// execute the Query on the server.
			result_set.ConnSetup(resp.ResultId, col_list, resp.RowCount);
			result_set.SetQueryTime(resp.QueryTimeMillis);
		}

		/// <summary>
		/// Called by ResultSet to Query a part of a result from the server.
		/// </summary>
		/// <param name="result_id"></param>
		/// <param name="start_row"></param>
		/// <param name="count_rows"></param>
		/// <returns>
		/// Returns a <see cref="IList"/> that represents the result from the server.
		/// </returns>
		internal ResultPart RequestResultPart(int result_id, int start_row, int count_rows) {
			return db_interface.GetResultPart(result_id, start_row, count_rows);
		}

		/// <summary>
		/// Requests a part of a streamable object from the server.
		/// </summary>
		/// <param name="result_id"></param>
		/// <param name="streamable_object_id"></param>
		/// <param name="offset"></param>
		/// <param name="len"></param>
		/// <returns></returns>
		internal StreamableObjectPart RequestStreamableObjectPart(int result_id, long streamable_object_id, long offset, int len) {
			return db_interface.GetStreamableObjectPart(result_id, streamable_object_id, offset, len);
		}

		/// <summary>
		/// Disposes of the server-side resources associated with the result 
		/// set with result_id.
		/// </summary>
		/// <param name="result_id"></param>
		/// <remarks>
		/// This should be called either before we start the download of a new result set, 
		/// or when we have finished with the resources of a result set.
		/// </remarks>
		internal void DisposeResult(int result_id) {
			// Clear the row cache.
			// It would be better if we only cleared row entries with this
			// table_id.  We currently clear the entire cache which means there will
			// be traffic created for other open result sets.
			//    Console.Out.WriteLine(result_id);
			//    row_cache.clear();
			// Only dispose if the connection is open
			if (!is_closed) {
				db_interface.DisposeResult(result_id);
			}
		}

		/// <summary>
		/// Adds a <see cref="ITriggerListener"/> that listens for all triggers events with 
		/// the name given.
		/// </summary>
		/// <param name="trigger_name"></param>
		/// <param name="listener"></param>
		/// <remarks>
		/// Triggers are created with the <c>CREATE TRIGGER</c> syntax.
		/// </remarks>
		internal void AddTriggerListener(String trigger_name, ITriggerListener listener) {
			lock (trigger_list) {
				trigger_list.Add(trigger_name);
				trigger_list.Add(listener);
			}
		}

		/// <summary>
		/// Removes the <see cref="ITriggerListener"/> for the given trigger name.
		/// </summary>
		/// <param name="trigger_name"></param>
		/// <param name="listener"></param>
		internal void RemoveTriggerListener(String trigger_name, ITriggerListener listener) {
			lock (trigger_list) {
				for (int i = trigger_list.Count - 2; i >= 0; i -= 2) {
					if (trigger_list[i].Equals(trigger_name) &&
						trigger_list[i + 1].Equals(listener)) {
						trigger_list.RemoveAt(i);
						trigger_list.RemoveAt(i);
					}
				}
			}
		}

		/// <summary>
		/// Creates a <see cref="StreamableObject"/> on the client side 
		/// given a <see cref="Stream"/>, and length and a type.
		/// </summary>
		/// <param name="x"></param>
		/// <param name="length"></param>
		/// <param name="type"></param>
		/// <remarks>
		/// When this method returns, a <see cref="StreamableObject"/> entry will be 
		/// added to the hold.
		/// </remarks>
		/// <returns></returns>
		internal StreamableObject CreateStreamableObject(Stream x, int length, ReferenceType type) {
			long ob_id;
			lock (s_object_hold) {
				ob_id = s_object_id;
				++s_object_id;
				// Add the stream to the hold and get the unique id
				s_object_hold[ob_id] = x;
			}
			// Create and return the StreamableObject
			return new StreamableObject(type, length, ob_id);
		}

		/// <summary>
		/// Removes the <see cref="StreamableObject"/> from the hold on the client.
		/// </summary>
		/// <param name="s_object"></param>
		/// <remarks>
		/// This should be called when the <see cref="DeveelDbCommand"/> closes.
		/// </remarks>
		internal void RemoveStreamableObject(StreamableObject s_object) {
			s_object_hold.Remove(s_object.Identifier);
		}

		// NOTE: For standalone apps, the thread that calls this will be a
		//   WorkerThread.
		//   For client/server apps, the thread that calls this will by the
		//   connection thread that listens for data from the server.
		public void OnDatabaseEvent(int event_type, String event_message) {
			if (event_type == 99) {
				if (trigger_thread == null) {
					trigger_thread = new TriggerDispatchThread(this);
					trigger_thread.Start();
				}
				trigger_thread.DispatchTrigger(event_message);
			} else {
				throw new ApplicationException("Unrecognised database event: " + event_type);
			}
		}

		/// <inheritdoc/>
		public new DeveelDbTransaction BeginTransaction() {
			//TODO: support multiple transactions...
			if (currentTransaction != null)
				throw new InvalidOperationException("A transaction was already opened on this connection.");

			bool autoCommit = false;
			if (AutoCommit) {
				AutoCommit = false;
				autoCommit = true;
			}

			int id;
			lock (typeof(DeveelDbConnection)) {
				id = transactionCounter++;
			}

			currentTransaction = new DeveelDbTransaction(this, id, autoCommit);
			return currentTransaction;
		}

		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) {
			if (isolationLevel != IsolationLevel.Serializable)
				throw new ArgumentException("Only SERIALIZABLE transactions are supported.");
			return BeginTransaction();
		}

		protected override void Dispose(bool disposing) {
			if (disposing)
				Close();

			base.Dispose(disposing);
		}

		public override System.Data.DataTable GetSchema() {
			return GetSchema(null);
		}

		public override System.Data.DataTable GetSchema(string collectionName) {
			if (collectionName == null)
				collectionName = DbMetaDataCollectionNames.MetaDataCollections;
			return GetSchema(collectionName, new string[0]);
		}

		public override System.Data.DataTable GetSchema(string collectionName, string[] restrictionValues) {
			if (metadata == null)
				metadata = new DatabaseMetadata(this);
			return metadata.GetSchema(collectionName, restrictionValues);
		}

		#region Implementation of IDbConnection

		/// <inheritdoc/>
		public override void Close() {
			if (state != ConnectionState.Closed) {
				bool success = InternalClose();
				lock (stateLock) {
					state = (success ? ConnectionState.Closed : ConnectionState.Broken);
				}
			}
		}

		///<summary>
		/// Closes this connection by calling the <see cref="IDisposable.Dispose"/> method 
		/// in the database interface.
		///</summary>
		internal virtual bool InternalClose() {
			try {
				try {
					if (currentTransaction != null)
						currentTransaction.Rollback();
				} catch(Exception) {
					// ignore any exception...
				}

				db_interface.Dispose();
				return true;
			} catch {
				return false;
			}
		}

		public override void ChangeDatabase(string databaseName) {
			//TODO: check if any command is in Executing state before...
			try {
				db_interface.ChangeDatabase(databaseName);
				connectionString.Database = databaseName;
			} catch(DataException) {
				throw;
			} catch(Exception e) {
				throw new DataException("An error occurred while changing the database: " + e.Message);
			}
		}

		protected override DbCommand CreateDbCommand() {
			return CreateCommand();
		}

		/// <inheritdoc/>
		public new DeveelDbCommand CreateCommand() {
			return new DeveelDbCommand(null, this);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="commandText"></param>
		/// <returns></returns>
		public DeveelDbCommand CreateCommand(string commandText) {
			return new DeveelDbCommand(commandText, this);
		}

		/// <summary>
		/// Creates a new <c>CALLBACK</c> trigger for the connection, having
		/// the given name and listening to event on the given database object 
		/// (eg. table, view, etc.), which listens to every event.
		/// </summary>
		/// <param name="name">The name of the trigger to create.</param>
		/// <param name="objectName">The name of the object for which to listen for events
		/// of data modifications (either <c>INSERT</c>, <c>DELETE</c> or <c>UPDATE</c>).</param>
		/// <remarks>
		/// Triggers are listeners to events of data modification happening on a
		/// database object they are attached to: every time a DML command modifies
		/// the contents of a database object, a callback trigger is fired to notify
		/// the client of the event.
		/// <para>
		/// Calling this method will not register a trigger on the database: this will
		/// be done when the method <see cref="DeveelDbTrigger.Subscribe"/> is called for
		/// the first time.
		/// </para>
		/// <para>
		/// Callback triggers are destroyed at the end of a connection to the database.
		/// </para>
		/// <para>
		/// To control on which event the trigger must be fired, the property 
		/// <see cref="DeveelDbTrigger.EventType"/> must be set.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="DeveelDbTrigger"/> that represents a
		/// <c>CALLBACK TRIGGER</c> on the connection.
		/// </returns>
		/// <seealso cref="DeveelDbTrigger"/>
		public DeveelDbTrigger CreateCallbackTrigger(string name, string objectName) {
			return new DeveelDbTrigger(this, name, objectName);
		}

		/// <summary>
		/// Creates a new <c>CALLBACK</c> trigger for the connection, having
		/// the given name and listening to event on the given database object 
		/// (eg. table, view, etc.), which listens to the events given.
		/// </summary>
		/// <param name="name">The name of the trigger to create.</param>
		/// <param name="objectName">The name of the object for which to listen for events
		/// of data modifications (either <c>INSERT</c>, <c>DELETE</c> or <c>UPDATE</c>).</param>
		/// <param name="listenTo">The <see cref="TriggerEventType">event types</see> to listen
		/// for on the database object.</param>
		/// <returns>
		/// Returns an instance of <see cref="DeveelDbTrigger"/> that represents a
		/// <c>CALLBACK TRIGGER</c> on the connection.
		/// </returns>
		/// <seealso cref="DeveelDbTrigger"/>
		public DeveelDbTrigger CreateCallbackTrigger(string name, string objectName, TriggerEventType listenTo) {
			DeveelDbTrigger trigger = CreateCallbackTrigger(name, objectName);
			trigger.EventType = listenTo;
			return trigger;
		}

		/// <summary>
		/// Toggles the <c>AUTO COMMIT</c> flag.
		/// </summary>
		public virtual bool AutoCommit {
			get { return auto_commit; }
			set {
				if (auto_commit == value)
					return;

				if (currentTransaction != null)
					throw new InvalidOperationException("A transaction is already opened.");

				// The SQL to write into auto-commit mode.
				if (value) {
					CreateCommand("SET AUTO COMMIT ON").ExecuteNonQuery();
					auto_commit = true;
				} else {
					CreateCommand("SET AUTO COMMIT OFF").ExecuteNonQuery();
					auto_commit = false;
				}
			}
		}

		/// <inheritdoc/>
		public override string ConnectionString {
			get { return Settings.ToString(); }
			set { Settings = new DeveelDbConnectionStringBuilder(value); }
		}

		internal DeveelDbConnectionStringBuilder Settings {
			get { return connectionString; }
			set {
				if (state != ConnectionState.Closed)
					throw new InvalidOperationException("The connection is not closed");
				connectionString = value;
				Init();
			}
		}

		/// <inheritdoc/>
		public override int ConnectionTimeout {
			get { return 0; }
		}

		/// <inheritdoc/>
		public override string Database {
			get { return Settings.Database; }
		}

		/// <inheritdoc/>
		public override ConnectionState State {
			get {
				lock (stateLock) {
					return state;
				}
			}
		}

		#endregion

		/// <summary>
		/// The thread that handles all dispatching of trigger events.
		/// </summary>
		private class TriggerDispatchThread {
			private readonly DeveelDbConnection conn;
			private readonly ArrayList trigger_messages_queue = new ArrayList();
			private readonly Thread thread;

			internal TriggerDispatchThread(DeveelDbConnection conn) {
				this.conn = conn;
				thread = new Thread(new ThreadStart(run));
				thread.IsBackground = true;
				thread.Name = "Trigger Dispatcher";
			}

			/// <summary>
			/// Dispatches a trigger message to the listeners.
			/// </summary>
			/// <param name="event_message"></param>
			internal void DispatchTrigger(String event_message) {
				lock (trigger_messages_queue) {
					trigger_messages_queue.Add(event_message);
					Monitor.PulseAll(trigger_messages_queue);
				}
			}

			// Thread run method
			private void run() {
				while (true) {
					try {
						String message;
						lock (trigger_messages_queue) {
							while (trigger_messages_queue.Count == 0) {
								try {
 									Monitor.Wait(trigger_messages_queue);
								} catch (ThreadInterruptedException) {
									/* ignore */
								}
							}
							message = (String)trigger_messages_queue[0];
							trigger_messages_queue.RemoveAt(0);
						}

						// 'message' is a message to process...
						// The format of a trigger message is:
						// "[trigger_name] [trigger_source] [trigger_fire_count]"
						//          Console.Out.WriteLine("TRIGGER EVENT: " + message);

						string[] tok = message.Split(' ');
						TriggerEventType event_type = (TriggerEventType) Convert.ToInt32(tok[0]);
						String trigger_name = tok[1];
						String trigger_source = tok[2];
						int trigger_fire_count = Convert.ToInt32(tok[3]);

						ArrayList fired_triggers = new ArrayList();
						// Create a list of Listener's that are listening for this trigger.
						lock (conn.trigger_list) {
							for (int i = 0; i < conn.trigger_list.Count; i += 2) {
								String to_listen_for = (String)conn.trigger_list[i];
								if (to_listen_for.Equals(trigger_name)) {
									ITriggerListener listener = (ITriggerListener)conn.trigger_list[i + 1];
									// NOTE, we can't call 'listener.OnTriggerFired' here because
									// it's not a good idea to call user code when we are
									// synchronized over 'trigger_list' (deadlock concerns).
									fired_triggers.Add(listener);
									fired_triggers.Add(new TriggerEventArgs(trigger_source, trigger_name, event_type, trigger_fire_count));
								}
							}
						}

						// Fire them triggers.
						for (int i = 0; i < fired_triggers.Count; i += 2) {
							ITriggerListener listener = (ITriggerListener)fired_triggers[i];
							TriggerEventArgs e = (TriggerEventArgs) fired_triggers[i + 1];
							listener.OnTriggerFired(e);
						}

					} catch (Exception t) {
						Console.Error.WriteLine(t.Message); 
						Console.Error.WriteLine(t.StackTrace);
					}

				}
			}

			public void Start() {
				thread.Start();
			}
		}

		/// <summary>
		/// The timeout for a query in seconds.
		/// </summary>
		internal static int QUERY_TIMEOUT = Int32.MaxValue;

		internal void SetState(ConnectionState connectionState) {
			lock (stateLock) {
				state = connectionState;
			}
		}

		internal void EndState() {
			lock (stateLock) {
				state = ConnectionState.Open;
			}
		}

#if !MONO
		private class PromotableConnection : IPromotableSinglePhaseNotification {
			public PromotableConnection(DeveelDbConnection conn) {
				this.conn = conn;
			}

			private readonly DeveelDbConnection conn;

			public byte[] Promote() {
				throw new NotImplementedException();
			}

			public void Initialize() {
				conn.currentTransaction = conn.BeginTransaction();
			}

			public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment) {
				if (conn.currentTransaction == null)
					throw new InvalidOperationException();

				conn.currentTransaction.Commit();
				singlePhaseEnlistment.Committed();
				conn.currentTransaction = null;
			}

			public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment) {
				if (conn.currentTransaction == null)
					throw new InvalidOperationException();

				conn.currentTransaction.Rollback();
				singlePhaseEnlistment.Aborted();
				conn.currentTransaction = null;
			}
		}
#endif
	}
}