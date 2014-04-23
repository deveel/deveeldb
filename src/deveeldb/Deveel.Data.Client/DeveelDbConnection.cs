// 
//  Copyright 2010-2014  Deveel
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
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Transactions;

using Deveel.Data.Configuration;
using Deveel.Data.Control;
using Deveel.Data.DbSystem;
using Deveel.Data.Protocol;
using Deveel.Data.Sql;

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
	public class DeveelDbConnection : DbConnection {
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
		private readonly Dictionary<string,ILocalBootable> localSessionMap = new Dictionary<string, ILocalBootable>();

		/// <summary>
		/// The string used to make this connection.
		/// </summary>
		private DeveelDbConnectionStringBuilder connectionString;

		/// <summary>
		/// Set to true if the connection is closed.
		/// </summary>
		private bool isClosed;

		/// <summary>
		/// Set to true if the connection is in auto-commit mode.
		/// (By default, auto_commit is enabled).
		/// </summary>
		private bool autoCommit;

		/// <summary>
		/// The interface to the database.
		/// </summary>
		private IDatabaseInterface dbInterface;

		/// <summary>
		/// The list of trigger listeners registered with the connection.
		/// </summary>
		private EventHandlerList triggerList;

		/// <summary>
		/// A Thread that handles all dispatching of trigger events to the client.
		/// </summary>
		private TriggerDispatchThread triggerThread;

		/// <summary>
		/// A mapping from a streamable object id to <see cref="Stream"/> used to 
		/// represent the object when being uploaded to the database engine.
		/// </summary>
		private Dictionary<object, Stream> sObjectHold;

		/// <summary>
		/// An unique id count given to streamable object being uploaded to the server.
		/// </summary>
		private long sObjectId;

		/// <summary>
		/// The current state of the connection;
		/// </summary>
		private ConnectionState state;

		/// <summary>
		/// If the user calls the method <see cref="BeginTransaction"/> this field
		/// is set and other calls to the method will trow an exception.
		/// </summary>
		internal DeveelDbTransaction currentTransaction;

		private static int transactionCounter;

		private DatabaseMetadata metadata;


		// For synchronization in this object,
		private readonly Object stateLock = new Object();

		internal DeveelDbConnection(string connectionString, IDatabaseInterface dbInterface, int cacheSize, int maxSize) {
			this.connectionString = new DeveelDbConnectionStringBuilder(connectionString);
			this.dbInterface = dbInterface;
			isClosed = true;
			autoCommit = true;
			triggerList = new EventHandlerList();
			RowCache = new RowCache(cacheSize, maxSize);
			sObjectHold = new Dictionary<object, Stream>();
			sObjectId = 0;
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

		internal void ChangeState(ConnectionState newState) {
			ChangeState(newState, null);
		}

		internal void ChangeState(ConnectionState newState, Exception error) {
			lock (stateLock) {
				base.OnStateChange(new StateChangeEventArgs(State, newState));
				state = newState;
			}
		}

		internal void EndState() {
			ChangeState(ConnectionState.Open);
		}

		private void Init() {
			int rowCacheSize;
			int maxRowCacheSize;

			// If we are to connect to a single user database running
			// within this runtime.
			if (IsLocal(connectionString.Host) ||
				IsInMemory(connectionString.Host)) {
				ConnectToLocal(IsInMemory(connectionString.Host));

				// Internal row cache setting are set small.
				rowCacheSize = 43;
				maxRowCacheSize = 4092000;
			} else {
				try {
					Thread.Sleep(85);
				} catch (ThreadInterruptedException) { /* ignore */ }

				// Make the connection
				dbInterface = new TCPStreamDatabaseInterface(connectionString.Host,
				                                              connectionString.Port,
				                                              connectionString.Database);

				// For remote connection, row cache uses more memory.
				rowCacheSize = 4111;
				maxRowCacheSize = 8192000;

			}

			isClosed = true;
			autoCommit = true;
			triggerList = new EventHandlerList();
			RowCache = new RowCache(rowCacheSize, maxRowCacheSize);
			sObjectHold = new Dictionary<object, Stream>();
			sObjectId = 0;
			state = ConnectionState.Closed;
		}

		private void ConnectToMemory() {
			lock (this) {
				var config = new DbConfig();
				config.StorageSystem(ConfigDefaultValues.HeapStorageSystem);

				controller = DbController.Create(config);
			}
		}

		private bool IsInMemory(string host) {
			return String.Equals("Heap", host, StringComparison.OrdinalIgnoreCase) ||
			       String.Equals("memory", host, StringComparison.OrdinalIgnoreCase);
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
		private void ConnectToLocal(bool memory) {
			lock (this) {
				// If the ILocalBootable object hasn't been created yet, do so now via
				// reflection.

				string sessionKey;

				if (!memory) {
					// The path to the configuration
					string rootPath = connectionString.Path;
					if (String.IsNullOrEmpty(rootPath))
						rootPath = Environment.CurrentDirectory;

					var controllerConfig = new DbConfig();
					controllerConfig.StorageSystem(ConfigDefaultValues.FileStorageSystem);
					controllerConfig.SetValue(ConfigKeys.BasePath, rootPath);
					controller = DbController.Create(controllerConfig);

					sessionKey = rootPath.ToLower();
				} else {
					var controllerConfig = new DbConfig();
					controllerConfig.StorageSystem(ConfigDefaultValues.HeapStorageSystem);
					controller = DbController.Create(controllerConfig);

					// TODO: compute this?
					sessionKey = "Memory";
				}

				// Is there already a local connection to this database?
				ILocalBootable localBootable;

				// No so create one and write it in the connection mapping
				if (!localSessionMap.TryGetValue(sessionKey, out localBootable)) {
					localBootable = CreateDefaultLocalBootable(controller, connectionString.Database);
					localSessionMap[sessionKey] = localBootable;
				}

				// Is the connection booted already?
				if (localBootable.IsBooted) {
					// Yes, so simply login.
					dbInterface = localBootable.Connect();
				} else {
					// Otherwise we need to boot the local database.

					bool createDb = connectionString.Create;
					bool createDbIfNotExist = connectionString.BootOrCreate;

					DbConfig config = new DbConfig(controller.Config);

					// Set the connection string variables
					config.SetValue(ConfigKeys.IgnoreIdentifiersCase, Settings.IgnoreIdentifiersCase);

					//TODO: set the additional configurations from the connection string
					/*
					string database_path = connectionString.AdditionalProperties["DatabasePath"] as string;
					if (database_path == null)
						database_path = Path.Combine(root_path, connectionString.Database);
					*/

					// Check if the database exists
					bool databaseExists = localBootable.CheckExists();

					// If database doesn't exist and we've been told to create it if it
					// doesn't exist, then set the 'create_db' flag.
					if (createDbIfNotExist && !databaseExists) {
						createDb = true;
					}

					// Error conditions;
					// If we are creating but the database already exists.
					if (createDb && databaseExists)
						throw new DataException("Can not create database because a database already exists.");

					// If we are booting but the database doesn't exist.
					if (!createDb && !databaseExists)
						throw new DataException("Can not find a database to start.  Either the database needs to " +
						                        "be created or the 'database_path' property of the configuration " +
						                        "must be set to the location of the data files.");

					// Are we creating a new database?
					if (createDb) {
						string username = connectionString.UserName;
						string password = connectionString.Password;

						dbInterface = localBootable.Create(username, password, config);
					}
						// Otherwise we must be logging onto a database,
					else {
						dbInterface = localBootable.Boot(config);
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
				Type c = typeof(DefaultLocalBootable);
				return (ILocalBootable)Activator.CreateInstance(c, new object[] { controller, databaseName });
			} catch (Exception) {
				// A lot of people ask us about this error so the message is verbose.
				throw new DataException("I was unable to find the class that manages local database " +
				                        "connections.  This means you may not have included the correct " +
				                        "library in your references.");
			}
		}

		private static bool IsLocal(string host) {
			return String.Equals(host, "{local}", StringComparison.OrdinalIgnoreCase) ||
			       String.Equals(host, "local", StringComparison.OrdinalIgnoreCase);
		}

		///<summary>
		/// Toggles whether this connection is handling identifiers as case
		/// insensitive or not. 
		///</summary>
		/// <remarks>
		/// If this is true then <see cref="DeveelDbDataReader.GetString">CreateString("app.id")</see> 
		/// will match against <c>APP.id</c>, etc.
		/// </remarks>
		internal virtual bool IsCaseInsensitiveIdentifiers {
			get { return Settings.IgnoreIdentifiersCase; }
		}

		/// <summary>
		/// Returns the row Cache object for this connection.
		/// </summary>
		internal RowCache RowCache { get; private set; }

		public override string DataSource {
			get { return IsLocal(Settings.Host) ? String.Empty : Settings.Host + ":" + Settings.Port; }
		}

		public override string ServerVersion {
			get { return IsLocal(Settings.Host) ? String.Empty : ((RemoteDatabaseInterface)dbInterface).ServerVersion.ToString(2); }
		}


		internal virtual bool InternalOpen() {
			string username = connectionString.UserName;
			string password = connectionString.Password;
			string defaultSchema = connectionString.Schema;

			if (username == null || username.Equals("") ||
				password == null || password.Equals("")) {
				throw new DataException("username or password have not been set.");
			}

			// Set the default schema to username if it's null
			if (defaultSchema == null) {
				defaultSchema = username;
			}

			try {
				if (dbInterface is TCPStreamDatabaseInterface)
					// Attempt to open a socket to the database.
					(dbInterface as TCPStreamDatabaseInterface).ConnectToDatabase();
			} catch(Exception e) {
				//TODO: log the exception...
				return false;
			}

			// Login with the username/password
			return dbInterface.Login(defaultSchema, username, password, OnDatabaseEvent);
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
			if (state != ConnectionState.Closed)
				throw new DataException("Unable to login to connection because it is open.");

			ChangeState(ConnectionState.Connecting);
			bool success = InternalOpen();
				ChangeState(success ? ConnectionState.Open : ConnectionState.Broken);

			if (success) {
				//TODO: separate from the Open procedure?
				// Determine if this connection is case insensitive or not,
				IDbCommand stmt = CreateCommand("SHOW CONNECTION_INFO");
				IDataReader rs = stmt.ExecuteReader();
				while (rs.Read()) {
					String key = rs.GetString(0);
					if (key.Equals("auto_commit")) {
						String val = rs.GetString(1);
						autoCommit = val.Equals("true");
					}
				}
				rs.Close();
			}
		}

		internal void PushStreamableObjectPart(ReferenceType type, long objectId, long length, byte[] buffer, long offset, int count) {
			dbInterface.PushStreamableObjectPart(type, objectId, length, buffer, offset, count);
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
						const int bufSize = 64 * 1024;

						StreamableObject sObject = (StreamableObject)vars[i];
						long offset = 0;
						ReferenceType type = sObject.Type;
						long totalLen = sObject.Size;
						long id = sObject.Identifier;
						byte[] buf = new byte[bufSize];

						// Get the InputStream from the StreamableObject hold
						object sobId = id;
						Stream iStream;
						if (!sObjectHold.TryGetValue(sobId, out iStream))
							throw new Exception("Assertion failed: Streamable object Stream is not available.");

						iStream.Seek(0, SeekOrigin.Begin);

						while (offset < totalLen) {
							// Fill the buffer
							int index = 0;
							int blockRead = (int)System.Math.Min(bufSize, (totalLen - offset));
							int toRead = blockRead;
							while (toRead > 0) {
								int count = iStream.Read(buf, index, toRead);
								if (count == 0)
									throw new IOException("Premature end of stream.");

								index += count;
								toRead -= count;
							}

							// Send the part of the streamable object to the database.
							dbInterface.PushStreamableObjectPart(type, id, totalLen, buf, offset, blockRead);
							// Increment the offset and upload the next part of the object.
							offset += blockRead;
						}

						// Remove the streamable object once it has been written
						sObjectHold.Remove(sobId);

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
		/// <param name="queries"></param>
		/// <param name="results">The consumer objects for the Query results.</param>
		/// <remarks>
		/// If a Query succeeds then we are guarenteed to know that size of the result set.
		/// <para>
		/// This method blocks until all of the _commands have been processed by the database.
		/// </para>
		/// </remarks>
		internal void ExecuteQueries(SqlQuery[] queries, ResultSet[] results) {
			// For each Query
			for (int i = 0; i < queries.Length; ++i) {
				ExecuteQuery(queries[i], results[i]);
			}
		}

		/// <summary>
		/// Sends the SQL string to the database to be executed.
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="resultSet">The consumer for the results from the database.</param>
		/// <remarks>
		/// We are guarenteed that if the Query succeeds that we know the size of the 
		/// result set and at least first first row of the set.
		/// <para>
		/// This method will block until we have received the result header information.
		/// </para>
		/// </remarks>
		internal void ExecuteQuery(SqlQuery sql, ResultSet resultSet) {
			UploadStreamableObjects(sql);
			// Execute the Query,
			IQueryResponse resp = dbInterface.ExecuteQuery(sql)[0];

			// The format of the result
			ColumnDescription[] colList = new ColumnDescription[resp.ColumnCount];
			for (int i = 0; i < colList.Length; ++i) {
				colList[i] = resp.GetColumnDescription(i);
			}
			// Set up the result set to the result format and update the time taken to
			// execute the Query on the server.
			resultSet.ConnSetup(resp.ResultId, colList, resp.RowCount);
			resultSet.SetQueryTime(resp.QueryTimeMillis);
		}

		/// <summary>
		/// Called by ResultSet to Query a part of a result from the server.
		/// </summary>
		/// <param name="resultId"></param>
		/// <param name="startRow"></param>
		/// <param name="countRows"></param>
		/// <returns>
		/// Returns a <see cref="IList"/> that represents the result from the server.
		/// </returns>
		internal ResultPart RequestResultPart(int resultId, int startRow, int countRows) {
			return dbInterface.GetResultPart(resultId, startRow, countRows);
		}

		/// <summary>
		/// Requests a part of a streamable object from the server.
		/// </summary>
		/// <param name="resultId"></param>
		/// <param name="streamableObjectId"></param>
		/// <param name="offset"></param>
		/// <param name="len"></param>
		/// <returns></returns>
		internal byte[] RequestStreamableObjectPart(int resultId, long streamableObjectId, long offset, int len) {
			return dbInterface.GetStreamableObjectPart(resultId, streamableObjectId, offset, len);
		}

		/// <summary>
		/// Disposes of the server-side resources associated with the result 
		/// set with result_id.
		/// </summary>
		/// <param name="resultId"></param>
		/// <remarks>
		/// This should be called either before we start the download of a new result set, 
		/// or when we have finished with the resources of a result set.
		/// </remarks>
		internal void DisposeResult(int resultId) {
			// Clear the row cache.
			// It would be better if we only cleared row entries with this
			// table_id.  We currently clear the entire cache which means there will
			// be traffic created for other open result sets.
			//    Console.Out.WriteLine(result_id);
			//    row_cache.clear();
			// Only dispose if the connection is open
			if (!isClosed) {
				dbInterface.DisposeResult(resultId);
			}
		}

		/// <summary>
		/// Adds a <see cref="TriggerEventHandler"/> that listens for all triggers events with 
		/// the name given.
		/// </summary>
		/// <param name="triggerName"></param>
		/// <param name="listener"></param>
		/// <remarks>
		/// Triggers are created with the <c>CREATE TRIGGER</c> syntax.
		/// </remarks>
		internal void AddTriggerListener(string triggerName, TriggerEventHandler listener) {
			lock (triggerList) {
				triggerList.AddHandler(triggerName, listener);
			}
		}

		/// <summary>
		/// Removes the <see cref="TriggerEventHandler"/> for the given trigger name.
		/// </summary>
		/// <param name="triggerName"></param>
		/// <param name="listener"></param>
		internal void RemoveTriggerListener(string triggerName, TriggerEventHandler listener) {
			lock (triggerList) {
				triggerList.RemoveHandler(triggerName, listener);
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
			long obId;
			lock (sObjectHold) {
				obId = sObjectId;
				++sObjectId;
				// Add the stream to the hold and get the unique id
				sObjectHold[obId] = x;
			}
			// Create and return the StreamableObject
			return new StreamableObject(type, length, obId);
		}

		/// <summary>
		/// Removes the <see cref="StreamableObject"/> from the hold on the client.
		/// </summary>
		/// <param name="sObject"></param>
		/// <remarks>
		/// This should be called when the <see cref="DeveelDbCommand"/> closes.
		/// </remarks>
		internal void RemoveStreamableObject(StreamableObject sObject) {
			sObjectHold.Remove(sObject.Identifier);
		}

		// NOTE: For standalone apps, the thread that calls this will be a
		//   WorkerThread.
		//   For client/server apps, the thread that calls this will by the
		//   connection thread that listens for data from the server.
		public void OnDatabaseEvent(int eventType, String eventMessage) {
			if (eventType == 99) {
				if (triggerThread == null) {
					triggerThread = new TriggerDispatchThread(this);
					triggerThread.Start();
				}
				triggerThread.DispatchTrigger(eventMessage);
			} else {
				throw new ApplicationException("Unrecognised database event: " + eventType);
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
				ChangeState((success ? ConnectionState.Closed : ConnectionState.Broken));
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

				dbInterface.Dispose();
				return true;
			} catch {
				return false;
			}
		}

		public override void ChangeDatabase(string databaseName) {
			//TODO: check if any command is in Executing state before...
			try {
				dbInterface.ChangeDatabase(databaseName);
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
			get { return autoCommit; }
			set {
				if (autoCommit == value)
					return;

				if (currentTransaction != null)
					throw new InvalidOperationException("A transaction is already opened.");

				// The SQL to write into auto-commit mode.
				if (value) {
					CreateCommand("SET AUTO COMMIT ON").ExecuteNonQuery();
					autoCommit = true;
				} else {
					CreateCommand("SET AUTO COMMIT OFF").ExecuteNonQuery();
					autoCommit = false;
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
			private readonly ArrayList triggerMessagesQueue = new ArrayList();
			private readonly Thread thread;

			internal TriggerDispatchThread(DeveelDbConnection conn) {
				this.conn = conn;
				thread = new Thread(new ThreadStart(Run));
				thread.IsBackground = true;
				thread.Name = "Trigger Dispatcher";
			}

			/// <summary>
			/// Dispatches a trigger message to the listeners.
			/// </summary>
			/// <param name="eventMessage"></param>
			internal void DispatchTrigger(String eventMessage) {
				lock (triggerMessagesQueue) {
					triggerMessagesQueue.Add(eventMessage);
					Monitor.PulseAll(triggerMessagesQueue);
				}
			}

			// Thread run method
			private void Run() {
				while (true) {
					try {
						String message;
						lock (triggerMessagesQueue) {
							while (triggerMessagesQueue.Count == 0) {
								try {
 									Monitor.Wait(triggerMessagesQueue);
								} catch (ThreadInterruptedException) {
									/* ignore */
								}
							}
							message = (String)triggerMessagesQueue[0];
							triggerMessagesQueue.RemoveAt(0);
						}

						// 'message' is a message to process...
						// The format of a trigger message is:
						// "[trigger_name] [trigger_source] [trigger_fire_count]"
						//          Console.Out.WriteLine("TRIGGER EVENT: " + message);

						string[] tok = message.Split(' ');
						TriggerEventType eventType = (TriggerEventType) Convert.ToInt32(tok[0]);
						string triggerName = tok[1];
						string triggerSource = tok[2];
						int triggerFireCount = Convert.ToInt32(tok[3]);

						// Create a list of Listener's that are listening for this trigger.
						lock (conn.triggerList) {
							TriggerEventHandler triggerHandler = conn.triggerList[triggerName] as TriggerEventHandler;
							if (triggerHandler != null)
								triggerHandler(conn, new TriggerEventArgs(triggerName, TableName.Resolve(triggerSource), eventType, triggerFireCount));
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
		internal const int QueryTimeout = Int32.MaxValue;

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