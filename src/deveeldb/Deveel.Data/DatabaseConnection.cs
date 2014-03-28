// 
//  Copyright 2010-2013  Deveel
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
using System.Collections.Generic;
using System.Data;

using Deveel.Data.Procedures;
using Deveel.Data.Query;
using Deveel.Data.Security;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// An object that represents a connection to a <see cref="Data.Database"/>.
	/// </summary>
	/// <remarks>
	/// This object handles all transactional queries and modifications to 
	/// the database.
	/// </remarks>
	public sealed partial class DatabaseConnection : IDisposable {
		/// <summary>
		/// The User that this connection has been made by.
		/// </summary>
		private User user;

		/// <summary>
		/// The Database object that this connection is on.
		/// </summary>
		private readonly Database database;

		/// <summary>
		///  A loop-back object that is managing this connection.  This typically is
		/// the session protocol.  This is notified of all connection events, such as
		/// triggers.
		/// </summary>
		private readonly TriggerCallback triggerCallback;

		/// <summary>
		/// The locking mechanism within this connection.
		/// </summary>
		private readonly LockingMechanism lockingMechanism;

		/// <summary>
		/// The <see cref="TableDataConglomerate"/> object that is used for 
		/// transactional access to the data.
		/// </summary>
		private readonly TableDataConglomerate conglomerate;

		/// <summary>
		/// The current Transaction that this connection is operating within.
		/// </summary>
		private Transaction transaction;

		/// <summary>
		/// The current <see cref="IDbConnection"/> object that can be used 
		/// to access the transaction internally.
		/// </summary>
		private IDbConnection dbConnection;

		/// <summary>
		/// If this is true then the database connection is in 'auto-commit' mode.
		/// This implies a COMMIT instruction is executed after every complete
		/// statement in the language grammar.  By default this is true.
		/// </summary>
		private bool autoCommit;

		/// <summary>
		/// The current transaction isolation level this connect is operating under.
		/// </summary>
		private IsolationLevel transactionIsolation;

		/// <summary>
		/// A flag which if set to true, will not allow 'commit' or 'rollback'
		/// commands on the transaction to occur and therefore prevent any open
		/// transaction from closing.  This is useful for restricting the ability
		/// of a stored procedure to close.
		/// </summary>
		private bool closeTransactionDisabled;

		/// <summary>
		/// The GrantManager object for this connection.
		/// </summary>
		private GrantManager grantManager;

		/// <summary>
		/// The list of all TableBackedCache objects that have been attached to this
		/// DatabaseConnection and are to be notified of transaction start/end
		/// events.
		/// </summary>
		private List<TableBackedCache> tableBackedCacheList;

		/// <summary>
		/// A local member that represents the static list of internal tables
		/// that represent connection specific properties such as username,
		/// connection, statistics, etc.
		/// </summary>
		private readonly ConnectionInternalTableInfo connectionInternalTableInfo;

		/// <summary>
		/// The <see cref="ILogger"/> object that we can use to log messages to.
		/// </summary>
		private readonly Logger logger;

		// ----- Local flags -----

		/// <summary>
		/// True if transactions through this connection generate an error when
		/// there is a dirty select on a table.
		/// </summary>
		private bool errorOnDirtySelect;

		/// <summary>
		/// True if this connection resolves identifiers case insensitive.
		/// </summary>
		private bool caseInsensitiveIdentifiers;

		internal DatabaseConnection(Database database, User user, TriggerCallback triggerCallback) {
			this.database = database;
			this.user = user;
			this.triggerCallback = triggerCallback;
			logger = database.Logger;
			conglomerate = database.Conglomerate;
			lockingMechanism = new LockingMechanism(logger);
			triggerEventBuffer = new List<TriggerEventArgs>();
			triggerEventList = new List<TriggerEventArgs>();
			autoCommit = true;

			currentSchema = Database.DefaultSchema;
			closeTransactionDisabled = false;

			tableBackedCacheList = new List<TableBackedCache>();

			connectionInternalTableInfo = new ConnectionInternalTableInfo(this);
			oldNewTableInfo = new OldAndNewInternalTableInfo(this);

			errorOnDirtySelect = database.System.TransactionErrorOnDirtySelect;
			caseInsensitiveIdentifiers = database.System.IgnoreIdentifierCase;

		}

		/// <summary>
		/// Gets the underlying transaction.
		/// </summary>
		/// <remarks>
		/// If none transaction was already open, it opens a new one
		/// with the underlying conglomerate.
		/// </remarks>
		private Transaction Transaction {
			get {
				lock (this) {
					if (transaction == null) {
						transaction = conglomerate.CreateTransaction();
						transaction.TransactionErrorOnDirtySelect = errorOnDirtySelect;
						// Internal tables (connection statistics, etc)
						transaction.AddInternalTableInfo(connectionInternalTableInfo);
						// OLD and NEW system tables (if applicable)
						transaction.AddInternalTableInfo(oldNewTableInfo);
						// Model views as tables (obviously)
						transaction.AddInternalTableInfo(ViewManager.CreateInternalTableInfo(view_manager, transaction));
						// Model procedures as tables
						transaction.AddInternalTableInfo(ProcedureManager.CreateInternalTableInfo(transaction));
						// Model sequences as tables
						transaction.AddInternalTableInfo(SequenceManager.CreateInternalTableInfo(transaction));
						// Model triggers as tables
						transaction.AddInternalTableInfo(ConnectionTriggerManager.CreateInternalTableInfo(transaction));

						// Notify any table backed caches that this transaction has started.
						foreach (TableBackedCache cache in tableBackedCacheList) {
							cache.OnTransactionStarted();
						}
					}
				}
				return transaction;
			}
		}

		/// <summary>
		/// Gets the database system object for this session.
		/// </summary>
		public DatabaseSystem System {
			get { return database.System; }
		}

		/// <summary>
		/// Gets the database object for this session.
		/// </summary>
		public Database Database {
			get { return database; }
		}

		/// <summary>
		/// Gets the conglomerate of this connection.
		/// </summary>
		internal TableDataConglomerate Conglomerate {
			get { return conglomerate; }
		}

		/// <summary>
		/// Gets an object that can be used to log debug messages to.
		/// </summary>
		public Logger Logger {
			get { return logger; }
		}

		/// <summary>
		/// Returns the user for this session.
		/// </summary>
		public User User {
			get { return user; }
			internal set {
				// This is necessary because we may want to temporarily change the 
				// user on this session to allow top level queries in a different 
				// privilege space.
				user = value;
			}
		}

		/// <summary>
		/// Returns the GrantManager object that manages grants for tables in the
		/// database for this connection/user.
		/// </summary>
		public GrantManager GrantManager {
			get { return grantManager; }
		}

		/// <summary>
		/// Gets or sets the <see cref="IsolationLevel"/> for the 
		/// session.
		/// </summary>
		public IsolationLevel TransactionIsolation {
			get { return transactionIsolation; }
			set {
				if (value != IsolationLevel.Serializable)
					throw new ApplicationException("Can not set transaction isolation to " + value);
				transactionIsolation = value;
			}
		}

		/// <summary>
		/// Gets or sets the auto-commit mode.
		/// </summary>
		/// <remarks>
		/// If this is <b>true</b> then the language layer must execute 
		/// a <c>COMMIT</c> after every statement.
		/// </remarks>
		public bool AutoCommit {
			get { return autoCommit; }
			set { autoCommit = value; }
		}

		/// <summary>
		/// Gets or sets a flag indicating if the session must ignore the case
		/// of the identifiers specified in queries.
		/// </summary>
		/// <remarks>
		/// In case insensitive mode the case of identifier strings is 
		/// not important.
		/// </remarks>
		public bool IsInCaseInsensitiveMode {
			get { return caseInsensitiveIdentifiers; }
		}

		/// <summary>
		/// Returns the locking mechanism within the context of the
		/// database session used to manages read/write locking.
		/// </summary>
		public LockingMechanism LockingMechanism {
			get { return lockingMechanism; }
		}

		/// <summary>
		/// Initializes this <see cref="DatabaseConnection"/> (possibly by initializing 
		/// state from the database).
		/// </summary>
		internal void Init() {
			// Create the grant manager for this connection.
			grantManager = new GrantManager(this);
			// Create the procedure manager for this connection.
			procedure_manager = new ProcedureManager(this);
			// Create the connection trigger manager object
			triggerManager = new ConnectionTriggerManager(this);
			// Create the view manager
			view_manager = new ViewManager(this);
		}

		/// <summary>
		/// Returns a freshly deserialized IQueryPlanNode object for the given view
		/// object.
		/// </summary>
		/// <param name="tableName">Name of the view to return the query plan node.</param>
		/// <returns></returns>
		internal IQueryPlanNode CreateViewQueryPlanNode(TableName tableName) {
			return view_manager.CreateViewQueryPlanNode(tableName);
		}

		/// <summary>
		/// Creates a <see cref="IDbConnection"/> object that can be 
		/// used as an ADO.NET interface to access the current transaction 
		/// of this <see cref="DatabaseConnection"/>.
		/// </summary>
		/// <remarks>
		/// There are a few important considerations when using the ADO.NET connection:
		/// <list type="bullet">
		///		<item>The returned <see cref="IDbConnection"/> does not allow 
		///		auto-commit to be set. It is intended to be used to issue commands 
		///		to this <see cref="DatabaseConnection"/> from inside a transaction so 
		///		auto-commit does not make sense.</item>
		///		<item>The returned object must only be accessed from the same worker
		///		thread that is currently accessing this <see cref="DatabaseConnection"/>.
		///		The returned <see cref="IDbConnection"/> is <b>not</b> multi-thread 
		///		capable.</item>
		///		<item>The <see cref="IDbConnection"/> returned here is invalidated 
		///		(disposed) when the current transaction is closed (committed or rolled 
		///		back).</item>
		///		<item>This method returns the same <see cref="IDbConnection"/> on multiple 
		///		calls to this method (while a transaction is open).</item>
		///		<item>The <see cref="DatabaseConnection"/> must be locked in 
		///		<see cref="LockingMode.Exclusive"/> mode or the queries will fail.</item>
		/// </list>
		/// </remarks>
		/// <returns></returns>
		public IDbConnection GetDbConnection() {
			if (dbConnection == null)
				dbConnection = InternalDbHelper.CreateDbConnection(User, this);
			return dbConnection;
		}

		/// <summary>
		/// Evaluates the expression to a bool value (true or false).
		/// </summary>
		/// <param name="exp"></param>
		/// <returns></returns>
		private static bool ToBooleanValue(Expression exp) {
			bool? b = exp.Evaluate(null, null, null).ToNullableBoolean();
			if (!b.HasValue)
				throw new StatementException("Expression does not evaluate to a bool (true or false).");
			return b.Value;
		}

		/// <summary>
		/// Attaches a <see cref="TableBackedCache"/> to 
		/// the session which is notified when a transaction is started and stopped, 
		/// and when the table being backed has changes made to it.
		/// </summary>
		/// <param name="cache">Cache to attach to the session.</param>
		internal void AttachTableBackedCache(TableBackedCache cache) {
			cache.AttachTo(conglomerate);
			tableBackedCacheList.Add(cache);
		}

		/// <summary>
		/// Notifies this transaction that a database object with the given name 
		/// has successfully been created.
		/// </summary>
		/// <param name="tableName"></param>
		internal void DatabaseObjectCreated(TableName tableName) {
			Transaction.OnDatabaseObjectCreated(tableName);
		}

		/// <summary>
		/// Notifies this transaction that a database object with the given name 
		/// has successfully been dropped.
		/// </summary>
		/// <param name="tableName"></param>
		internal void DatabaseObjectDropped(TableName tableName) {
			Transaction.OnDatabaseObjectDropped(tableName);
		}

		/// <summary>
		/// If the given table name is a reserved name, then we must substitute it
		/// with its correct form.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns></returns>
		static TableName SubstituteReservedTableName(TableName tableName) {
			// We do not allow tables to be created with a reserved name
			String name = tableName.Name;
			if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTable;
			if (String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTable;
			return tableName;
		}


		/// <summary>
		/// Generates an exception if the name of the table is reserved and the
		/// creation of the table should be prevented.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// For example, the table names <c>OLD</c> and <c>NEW</c> are reserved.
		/// </remarks>
		internal static void CheckAllowCreate(TableName tableName) {
			// We do not allow tables to be created with a reserved name
			String name = tableName.Name;
			if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0 ||
				String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0) {
				throw new StatementException("Table name '" + tableName + "' is reserved.");
			}
		}

		/// <summary>
		/// Allocates a new large object in the blob store in the underlying conglomerate
		/// for the given type and size.
		/// </summary>
		/// <param name="type"></param>
		/// <param name="objectSize"></param>
		/// <remarks>
		/// The blob data must be written through the <see cref="IRef"/>
		/// after the large object is created. Once the data has been written 
		/// <see cref="IRef.Complete"/> is called.
		/// <para>
		/// Once a large object is created and written to, it may be allocated 
		/// in one or more tables in the conglomerate.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public IRef CreateNewLargeObject(ReferenceType type, long objectSize) {
			// Enable compression for string types (but not binary types).
			if (type == ReferenceType.AsciiText || 
				type == ReferenceType.UnicodeText) {
				type |= ReferenceType.Compressed;
			}
			return conglomerate.CreateNewLargeObject(type, objectSize);
		}

		/// <summary>
		/// Tells the underlying conglomerate to flush the blob store.
		/// </summary>
		/// <remarks>
		/// This should be called after one or more blobs have been created and 
		/// the data for the blob(s) are set. It is an important step to perform 
		/// <b>after</b> blobs have been written.
		/// <para>
		/// If this is not called and the database closes (or crashes) before a flush
		/// occurs then the blob may not be recoverable.
		/// </para>
		/// </remarks>
		[Obsolete("Deprecated: no longer necessary", false)]
		public void FlushBlobStore() {
			conglomerate.FlushBlobStore();
		}

		/// <summary>
		/// Returns a <see cref="ITableQueryInfo"/> that describes the 
		/// characteristics of a table including the name, the columns and the 
		/// query plan to produce the table.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="aliasedAs">Used to overwrite the default name of 
		/// the table object.</param>
		/// <remarks>
		/// This object can be used to resolve information about a 
		/// particular table, and to evaluate the query plan to produce 
		/// the table itself.
		/// <para>
		/// This produces <see cref="ITableQueryInfo"/> objects for all table 
		/// objects in the database including data tables and views.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public ITableQueryInfo GetTableQueryInfo(TableName tableName, TableName aliasedAs) {
			// Produce the data table info for this database object.
			DataTableInfo tableInfo = GetTableInfo(tableName);
			// If the table is aliased, set a new DataTableInfo with the given name
			if (aliasedAs != null) {
				tableInfo = tableInfo.Clone(aliasedAs);
				tableInfo.IsReadOnly = true;
			}
			
			return new TableQueryInfo(this, tableInfo, tableName, aliasedAs);

		}

		/// <summary>
		/// Creates a <see cref="IQueryPlanNode"/> to fetch the given table 
		/// object from the session.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="aliasedName"></param>
		/// <returns></returns>
		public IQueryPlanNode CreateObjectFetchQueryPlan(TableName tableName, TableName aliasedName) {
			string tableType = GetTableType(tableName);
			if (tableType.Equals("VIEW"))
				return new FetchViewNode(tableName, aliasedName);
			return new FetchTableNode(tableName, aliasedName);
		}

		// ---------- Schema management and constraint methods ----------
		// Methods that handle getting/setting schema information such as;
		// * Creating/dropping/querying schema
		// * Creating/dropping/querying constraint information including;
		//     check constraints, unique constraints, primary key constraints,
		//     foreign key constraints, etc.

		// NOTE: These methods are copied because they simply call through to the
		//   Transaction implementation of the method with the same signature.

		private void CheckExclusive() {
			if (!LockingMechanism.IsInExclusiveMode) {
				throw new ApplicationException("Assertion failed: Expected to be in exclusive mode.");
			}
		}

		// ---------- User-Defined Types management ----------

		public UserType GetUserType(TableName name) {
			return Transaction.GetUserType(name);
		}

		public void CreateUserType(UserType type) {
			Transaction.CreateUserType(type);
		}

		public void DropUserType(TableName name) {
			Transaction.DropUserType(name);
		}

		public bool UserTypeExists(TableName name) {
			return Transaction.UserTypeExists(name);
		}


		/// <summary>
		/// Private method that disposes the current transaction.
		/// </summary>
		private void DisposeTransaction() {
			// Set the transaction to null
			transaction = null;
			// Fire any pending trigger events in the trigger buffer.
			FirePendingTriggerEvents();
			// Clear the trigger events in this object
			triggerEventList.Clear();

			// Notify any table backed caches that this transaction has finished.
			foreach (TableBackedCache cache in tableBackedCacheList) {
				cache.OnTransactionFinished();
			}
		}

		/// <summary>
		/// Tries to commit the current transaction.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> It's guarenteed that the transaction will be closed even if a
		/// transaction exception occurs.
		/// <para>
		/// Lock is implied on this method, because the locking mechanism
		/// should be exclusive when this is called.
		/// </para>
		/// </remarks>
		/// <exception cref="TransactionException">
		/// If the transaction can not be committed because there were concurrent 
		/// changes that interfered with each other (the transaction is rolled-back) 
		/// or if the session is not allowed to close the transaction.
		/// </exception>
		public void Commit() {
			// Are we currently allowed to commit/rollback?
			if (closeTransactionDisabled)
				throw new Exception("Commit is not allowed.");

			if (user != null)
				user.RefreshLastCommandTime();

			// NOTE, always connection exclusive op.
			LockingMechanism.Reset();
			tablesCache.Clear();

			if (transaction != null) {
				try {
					// Close and commit the transaction
					transaction.Commit();

					// Fire all SQL action level triggers that were generated on actions.
					triggerManager.FlushTriggerEvents(triggerEventList);
				} finally {
					// Dispose the current transaction
					DisposeTransaction();
				}
			}
		}

		/// <summary>
		/// Rolls back the current transaction operating within 
		/// the session.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> It's guarenteed that the transaction will be closed even if an
		/// exception occurs.
		/// <para>
		/// Locking is implied on this method, because the locking mechanism
		/// should be exclusive when this is called.
		/// </para>
		/// </remarks>
		/// <exception cref="TransactionException">
		/// If the session is not allowed to close the underlying transaction.
		/// </exception>
		public void Rollback() {
			// Are we currently allowed to commit/rollback?
			if (closeTransactionDisabled) {
				throw new Exception("Rollback is not allowed.");
			}

			if (user != null) {
				user.RefreshLastCommandTime();
			}

			// NOTE, always connection exclusive op.
			tablesCache.Clear();

			if (transaction != null) {
				LockingMechanism.Reset();
				try {
					transaction.Rollback();
				} finally {
					// Dispose the current transaction
					DisposeTransaction();
					// Dispose the ado.net connection
					if (dbConnection != null) {
						try {
							InternalDbHelper.DisposeDbConnection(dbConnection);
						} catch (Exception e) {
							Logger.Error(this, "Error disposing internal ADO.NET connection.");
							Logger.Error(this, e);
							// We don't wrap this exception
						}
						dbConnection = null;
					}
				}
			}
		}

		/// <summary>
		/// Closes this database connection.
		/// </summary>
		public void Close() {
			try {
				Rollback();
			} catch (Exception e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
			} finally {
				if (tableBackedCacheList != null) {
					try {
						foreach (TableBackedCache cache in tableBackedCacheList) {
							cache.DetatchFrom(conglomerate);
						}
						tableBackedCacheList = null;
					} catch (Exception e) {
						Console.Error.WriteLine(e.Message);
						Console.Error.WriteLine(e.StackTrace);
					}
				}
				// Remove any trigger listeners set for this connection,
				TriggerManager.ClearCallbackTriggers();
			}
		}


		/// <summary>
		/// A list of DataTableInfo system table definitions for tables internal to
		/// the database connection.
		/// </summary>
		private readonly static DataTableInfo[] InternalInfoList;

		static DatabaseConnection() {
			InternalInfoList = new DataTableInfo[5];
			InternalInfoList[0] = GTStatisticsDataSource.DataTableInfo;
			InternalInfoList[1] = GTConnectionInfoDataSource.DataTableInfo;
			InternalInfoList[2] = GTCurrentConnectionsDataSource.DataTableInfo;
			InternalInfoList[3] = GTSQLTypeInfoDataSource.DataTableInfo;
			InternalInfoList[4] = GTPrivMapDataSource.DataTableInfo;
		}

		private class TableQueryInfo : ITableQueryInfo {
			private readonly DatabaseConnection conn;
			private readonly DataTableInfo tableInfo;
			private readonly TableName tableName;
			private readonly TableName aliasedAs;

			public TableQueryInfo(DatabaseConnection conn, DataTableInfo tableInfo, TableName tableName, TableName aliasedAs) {
				this.conn = conn;
				this.tableInfo = tableInfo;
				this.aliasedAs = aliasedAs;
				this.tableName = tableName;
			}

			public DataTableInfo TableInfo {
				get { return tableInfo; }
			}

			public IQueryPlanNode QueryPlanNode {
				get { return conn.CreateObjectFetchQueryPlan(tableName, aliasedAs); }
			}
		}

		/// <summary>
		/// An internal table info object that handles tables internal to a
		/// DatabaseConnection object.
		/// </summary>
		private class ConnectionInternalTableInfo : InternalTableInfo {
			private readonly DatabaseConnection conn;

			public ConnectionInternalTableInfo(DatabaseConnection conn)
				: base("SYSTEM TABLE", InternalInfoList) {
				this.conn = conn;
			}

			public override ITableDataSource CreateInternalTable(int index) {
				if (index == 0)
					return new GTStatisticsDataSource(conn).Init();
				if (index == 1)
					return new GTConnectionInfoDataSource(conn).Init();
				if (index == 2)
					return new GTCurrentConnectionsDataSource(conn).Init();
				if (index == 3)
					return new GTSQLTypeInfoDataSource(conn).Init();
				if (index == 4)
					return new GTPrivMapDataSource(conn);
				throw new Exception();
			}

		}

		/// <inheritdoc/>
		public void Dispose() {
			Close();
		}
	}
}