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

using Deveel.Data.Routines;
using Deveel.Data.Query;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An object that represents a connection to a <see cref="DbSystem.Database"/>.
	/// </summary>
	/// <remarks>
	/// This object handles all transactional queries and modifications to 
	/// the database.
	/// </remarks>
	public sealed class DatabaseConnection : IDisposable {
		/// <summary>
		///  A loop-back object that is managing this connection.  This typically is
		/// the session protocol.  This is notified of all connection events, such as
		/// triggers.
		/// </summary>
		private readonly TriggerCallback triggerCallback;

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
		private readonly ConnectionInternalTableContainer connIntTableContainer;

		// ----- Local flags -----

		/// <summary>
		/// True if transactions through this connection generate an error when
		/// there is a dirty select on a table.
		/// </summary>
		private bool errorOnDirtySelect;

		internal DatabaseConnection(Database database, User user, TriggerCallback triggerCallback) {
			this.Database = database;
			this.User = user;
			this.triggerCallback = triggerCallback;
			Logger = database.Context.Logger;
			conglomerate = database.Conglomerate;
			LockingMechanism = new LockingMechanism(Logger);
			triggerEventBuffer = new List<TriggerEventArgs>();
			triggerEventList = new List<TriggerEventArgs>();
			AutoCommit = true;

			currentSchema = Database.DefaultSchema;
			closeTransactionDisabled = false;

			tableBackedCacheList = new List<TableBackedCache>();

			connIntTableContainer = new ConnectionInternalTableContainer(this);
			oldNewContainer = new OldAndNewTableContainer(this);

			errorOnDirtySelect = database.Context.TransactionErrorOnDirtySelect;
			IsInCaseInsensitiveMode = database.Context.IgnoreIdentifierCase;

		}

		/// <summary>
		/// Gets the underlying transaction.
		/// </summary>
		/// <remarks>
		/// If none transaction was already open, it opens a new one
		/// with the underlying conglomerate.
		/// </remarks>
		private ITransaction Transaction {
			get {
				lock (this) {
					if (transaction == null) {
						transaction = conglomerate.CreateTransaction();
						transaction.TransactionErrorOnDirtySelect = errorOnDirtySelect;
						// Internal tables (connection statistics, etc)
						transaction.AddInternalTableInfo(connIntTableContainer);
						// OLD and NEW system tables (if applicable)
						transaction.AddInternalTableInfo(oldNewContainer);
						// Model views as tables (obviously)
						transaction.AddInternalTableInfo(ViewManager.CreateInternalTableInfo(viewManager, transaction));
						// Model procedures as tables
						transaction.AddInternalTableInfo(RoutinesManager.CreateInternalTableInfo(transaction));
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

		private ICommitableTransaction CommittableTransaction {
			get {
				var committable = Transaction as ICommitableTransaction;
				if (committable == null)
					throw new InvalidOperationException("A transaction was not open or it's not commitable.");

				return committable;
			}
		}

		/// <summary>
		/// Gets the database system object for this session.
		/// </summary>
		public DatabaseContext Context {
			get { return Database.Context; }
		}

		/// <summary>
		/// Gets the database object for this session.
		/// </summary>
		public Database Database { get; private set; }

		/// <summary>
		/// Gets an object that can be used to log debug messages to.
		/// </summary>
		public ILogger Logger { get; private set; }

		/// <summary>
		/// Returns the user for this session.
		/// </summary>
		public User User { get; private set; }

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
		public bool AutoCommit { get; set; }

		/// <summary>
		/// Gets or sets a flag indicating if the session must ignore the case
		/// of the identifiers specified in queries.
		/// </summary>
		/// <remarks>
		/// In case insensitive mode the case of identifier strings is 
		/// not important.
		/// </remarks>
		public bool IsInCaseInsensitiveMode { get; private set; }

		/// <summary>
		/// Returns the locking mechanism within the context of the
		/// database session used to manages read/write locking.
		/// </summary>
		public LockingMechanism LockingMechanism { get; private set; }

		/// <summary>
		/// Initializes this <see cref="DatabaseConnection"/> (possibly by initializing 
		/// state from the database).
		/// </summary>
		internal void Init() {
			// Create the grant manager for this connection.
			grantManager = new GrantManager(this);
			// Create the procedure manager for this connection.
			routinesManager = new RoutinesManager(this);
			// Create the connection trigger manager object
			triggerManager = new ConnectionTriggerManager(this);
			// Create the view manager
			viewManager = new ViewManager(this);
		}

		/// <summary>
		/// Returns a freshly deserialized IQueryPlanNode object for the given view
		/// object.
		/// </summary>
		/// <param name="tableName">Name of the view to return the query plan node.</param>
		/// <returns></returns>
		internal IQueryPlanNode CreateViewQueryPlanNode(TableName tableName) {
			return viewManager.CreateViewQueryPlanNode(tableName);
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
		/// <param name="context"></param>
		/// <returns></returns>
		private static bool ToBooleanValue(Expression exp, IQueryContext context) {
			var value = exp.Evaluate(null, null, context);
			if (value.IsNull)
				throw new StatementException("Expression does not evaluate to a bool (true or false).");

			if (value.TType is TBooleanType)
				return value.ToBoolean();

			if (value.TType is TNumericType) {
				var iValue = value.ToBigNumber().ToInt32();
				if (iValue == 0)
					return false;
				if (iValue == 1)
					return true;
			}

			throw new StatementException("Expression does not evaluate to a bool (true or false).");
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
			CommittableTransaction.OnDatabaseObjectCreated(tableName);
		}

		/// <summary>
		/// Notifies this transaction that a database object with the given name 
		/// has successfully been dropped.
		/// </summary>
		/// <param name="tableName"></param>
		internal void DatabaseObjectDropped(TableName tableName) {
			CommittableTransaction.OnDatabaseObjectDropped(tableName);
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

			if (User != null)
				User.RefreshLastCommandTime();

			// NOTE, always connection exclusive op.
			LockingMechanism.Reset();
			tablesCache.Clear();

			if (transaction != null) {
				try {
					// Close and commit the transaction
					CommittableTransaction.Commit();

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

			if (User != null) {
				User.RefreshLastCommandTime();
			}

			// NOTE, always connection exclusive op.
			tablesCache.Clear();

			if (transaction != null) {
				LockingMechanism.Reset();
				try {
					CommittableTransaction.Rollback();
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

		#region Schemata

		/// <summary>
		/// The name of the schema that this connection is currently in.  If the
		/// schema is "" then this connection is in the default schema (effectively
		/// no schema).
		/// </summary>
		private string currentSchema;

		/// <summary>
		/// Gets or sets the name of the schema that this connection is within.
		/// </summary>
		public string CurrentSchema {
			get { return currentSchema; }
			set { currentSchema = value; }
		}

		/// <summary>
		/// Changes the default schema to the given schema.
		/// </summary>
		/// <param name="schemaName"></param>
		public void SetDefaultSchema(string schemaName) {
			bool ignoreCase = IsInCaseInsensitiveMode;
			SchemaDef schema = ResolveSchemaCase(schemaName, ignoreCase);
			if (schema == null)
				throw new ApplicationException("Schema '" + schemaName + "' does not exist.");

			// Set the default schema for this connection
			CurrentSchema = schema.Name;
		}

		public void CreateSchema(String name, String type) {
			// Assert
			CheckExclusive();
			CommittableTransaction.CreateSchema(name, type);
		}

		public void DropSchema(String name) {
			// Assert
			CheckExclusive();
			CommittableTransaction.DropSchema(name);
		}

		public bool SchemaExists(String name) {
			return Transaction.SchemaExists(name);
		}

		public SchemaDef ResolveSchemaCase(String name, bool ignoreCase) {
			return Transaction.ResolveSchemaCase(name, ignoreCase);
		}

		/**
		 * Convenience - returns the SchemaDef object given the name of the schema.
		 * If identifiers are case insensitive, we resolve the case of the schema
		 * name also.
		 */
		///<summary>
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public SchemaDef ResolveSchemaName(String name) {
			bool ignoreCase = IsInCaseInsensitiveMode;
			return ResolveSchemaCase(name, ignoreCase);
		}

		public SchemaDef[] GetSchemaList() {
			return Transaction.GetSchemaList();
		}

		#endregion

		#region Tables

		/// <summary>
		/// A Hashtable of DataTable objects that have been created within this connection.
		/// </summary>
		private readonly Dictionary<TableName, DataTable> tablesCache = new Dictionary<TableName, DataTable>();

		/// <summary>
		/// Gets an array of <see cref="TableName"/> that contains the 
		/// list of database tables visible by the underlying transaction.
		/// </summary>
		/// <remarks>
		/// The list returned represents all the queriable tables in
		/// the database.
		/// </remarks>
		public TableName[] Tables {
			get { return Transaction.GetTables(); }
		}

		/// <summary>
		/// Checks the existence of a table within the underlying transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to check.</param>
		/// <remarks>
		/// This method checks if the table exists within the <see cref="CurrentSchema"/>
		/// of the session.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if a table with the given <paramref name="tableName"/>
		/// exists within the underlying transaction, otherwise <b>false</b>.
		/// </returns>
		public bool TableExists(string tableName) {
			return TableExists(new TableName(currentSchema, tableName));
		}

		/// <summary>
		/// Checks the existence of a table within the underlying transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to check.</param>
		/// <returns>
		/// Returns <b>true</b> if a table with the given <paramref name="tableName"/>
		/// exists within the underlying transaction, otherwise <b>false</b>.
		/// </returns>
		public bool TableExists(TableName tableName) {
			tableName = SubstituteReservedTableName(tableName);
			return Transaction.TableExists(tableName);
		}

		/// <summary>
		/// Gets the type of he given table.
		/// </summary>
		/// <param name="tableName">Name of the table to get the type.</param>
		/// <remarks>
		/// Currently this is either <i>TABLE</i> or <i>VIEW</i>.
		/// </remarks>
		/// <returns>
		/// Returns a string describing the type of the table identified by the
		/// given <paramref name="tableName"/>.
		/// </returns>
		/// <exception cref="StatementException">
		/// If none table with the given <paramref name="tableName"/> was found 
		/// in the underlying transaction.</exception>
		public string GetTableType(TableName tableName) {
			tableName = SubstituteReservedTableName(tableName);
			return Transaction.GetTableType(tableName);
		}

		/// <summary>
		/// Attempts to resolve the given table name to its correct case assuming
		/// the table name represents a case insensitive version of the name.
		/// </summary>
		/// <param name="tableName">Table name to resolve.</param>
		/// <remarks>
		/// For example, <c>aPP.CuSTOMer</c> may resolve to <c>default.Customer</c>.
		/// If the table name can not resolve to a valid identifier it returns 
		/// the input table name
		/// The actual presence of the table should always be checked by 
		/// calling <see cref="TableExists(TableName)"/> after the  method 
		/// returns.
		/// </remarks>
		/// <returns>
		/// Returns a properly formatted <see cref="TableName"/> if was able to
		/// resolve the given <paramref name="tableName"/>, otherwise returns
		/// the input table name.
		/// </returns>
		public TableName TryResolveCase(TableName tableName) {
			tableName = SubstituteReservedTableName(tableName);
			tableName = Transaction.TryResolveCase(tableName);
			return tableName;
		}

		/// <summary>
		/// Resolves a table name.
		/// </summary>
		/// <param name="name">Name of the table to resolve.</param>
		/// <remarks>
		/// If the schema part of the table name is not present then it is set 
		/// to the <see cref="CurrentSchema"/> of the database session.
		/// If the database is ignoring the case then this will correctly resolve 
		/// the table to the cased version of the table name.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="TableName"/> representing the properly
		/// formatted table name.
		/// </returns>
		public TableName ResolveTableName(string name) {
			TableName tableName = TableName.Resolve(CurrentSchema, name);
			tableName = SubstituteReservedTableName(tableName);
			if (IsInCaseInsensitiveMode) {
				// Try and resolve the case of the table name,
				tableName = TryResolveCase(tableName);
			}
			return tableName;
		}

		/// <summary>
		/// Resolves the given string to a table name
		/// </summary>
		/// <param name="name">Table name to resolve.</param>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the reference is ambigous or
		/// </exception>
		public TableName ResolveToTableName(string name) {
			TableName tableName = TableName.Resolve(CurrentSchema, name);
			if (String.Compare(tableName.Name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTable;
			if (String.Compare(tableName.Name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTable;

			return Transaction.ResolveToTableName(CurrentSchema, name, IsInCaseInsensitiveMode);

		}

		/// <summary>
		/// Gets the meta informations for the given table.
		/// </summary>
		/// <param name="name">Name of the table to return the 
		/// meta informations.</param>
		/// <returns>
		/// Returns the <see cref="DataTableInfo"/> representing the meta 
		/// informations for the tabl identified by <paramref name="name"/> 
		/// if found, otherwise <b>null</b>.
		/// </returns>
		public DataTableInfo GetTableInfo(TableName name) {
			name = SubstituteReservedTableName(name);
			return Transaction.GetTableInfo(name);
		}

		/// <summary>
		/// Gets the table for the given name.
		/// </summary>
		/// <param name="name">Name of the table to return.</param>
		/// <returns>
		/// Returns a <see cref="DataTable"/> that represents the table 
		/// identified by <paramref name="name"/>, otherwise returns 
		/// <b>null</b>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If none table was found for the given <paramref name="name"/>.
		/// </exception>
		public DataTable GetTable(TableName name) {
			name = SubstituteReservedTableName(name);

			try {
				// Special handling of NEW and OLD table, we cache the DataTable in the
				// OldNewTableState object,
				if (name.Equals(SystemSchema.OldTriggerTable)) {
					return currentOldNewState.OldDataTable ??
						   (currentOldNewState.OldDataTable = new DataTable(this, Transaction.GetTable(name)));
				}
				if (name.Equals(SystemSchema.NewTriggerTable)) {
					return currentOldNewState.NewDataTable ??
						   (currentOldNewState.NewDataTable = new DataTable(this, Transaction.GetTable(name)));
				}

				// Ask the transaction for the table
				ITableDataSource table = Transaction.GetTable(name);

				// if not found in the transaction return null
				if (table == null)
					return null;

				// Is this table in the tables_cache?
				DataTable dtable;
				if (!tablesCache.TryGetValue(name, out dtable)) {
					// No, so wrap it around a Datatable and WriteByte it in the cache
					dtable = new DataTable(this, table);
					tablesCache[name] = dtable;
				}

				// Return the DataTable
				return dtable;

			} catch (DatabaseException e) {
				Logger.Error(this, e);
				throw new ApplicationException("Database Exception: " + e.Message, e);
			}

		}

		/// <summary>
		/// Gets the table for the given name.
		/// </summary>
		/// <param name="tableName">Name of the table to return.</param>
		/// <remarks>
		/// This method uses the <see cref="CurrentSchema"/> to get the table.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="DataTable"/> that represents the table 
		/// identified by <paramref name="tableName"/>, otherwise returns <b>null</b>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If none table was found for the given <paramref name="tableName"/>.
		/// </exception>
		public DataTable GetTable(string tableName) {
			return GetTable(new TableName(currentSchema, tableName));
		}

		/// <summary>
		/// Creates a new temporary table within the context of the transaction.
		/// </summary>
		/// <param name="tableInfo">Table meta informations for creating the table.</param>
		/// <remarks>
		/// A temporary table is a fully functional table, which persists for all the lifetime
		/// of a transaction and that is disposed (both structure and data) at the end of the
		/// parent transaction.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public void CreateTemporaryTable(DataTableInfo tableInfo) {
			CheckAllowCreate(tableInfo.TableName);
			CommittableTransaction.CreateTemporaryTable(tableInfo);
		}

		/// <summary>
		/// Creates a new table within the context of the transaction.
		/// </summary>
		/// <param name="tableInfo">Table meta informations for creating the table.</param>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public void CreateTable(DataTableInfo tableInfo) {
			CheckAllowCreate(tableInfo.TableName);
			CommittableTransaction.CreateTable(tableInfo);
		}

		/// <summary>
		/// Creates a new table within this transaction with the given 
		/// sector size.
		/// </summary>
		/// <param name="tableInfo">Meta informations used to create the table.</param>
		/// <param name="dataSectorSize">Size of data sectors of the table.</param>
		/// <param name="indexSectorSize">Size of the index sectors of the table.</param>
		/// <remarks>
		/// This should only be used as very fine grain optimization for 
		/// creating tables. 
		/// If in the future the underlying table model is changed so that the given
		/// <paramref name="dataSectorSize"/> value is unapplicable, then the value 
		/// will be ignored.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If a table with the same name (specified by <paramref name="tableInfo"/>) 
		/// already exists.
		/// </exception>
		public void CreateTable(DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			CheckAllowCreate(tableInfo.TableName);
			CommittableTransaction.CreateTable(tableInfo, dataSectorSize, indexSectorSize);
		}

		/// <summary>
		/// Alters a table within the underlying transaction.
		/// </summary>
		/// <param name="tableInfo">Table metadata informations for aletring the table</param>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public void UpdateTable(DataTableInfo tableInfo) {
			CheckAllowCreate(tableInfo.TableName);
			CommittableTransaction.AlterTable(tableInfo.TableName, tableInfo);
		}

		/// <summary>
		/// Alters a table within the underlying transaction.
		/// </summary>
		/// <param name="tableInfo">Table metadata informations for altering 
		/// the table.</param>
		/// <param name="dataSectorSize"></param>
		/// <param name="indexSectorSize"></param>
		/// <remarks>
		/// This should only be used as very fine grain optimization
		/// for creating tables. If in the future the underlying table model is
		/// changed so that the given <paramref name="dataSectorSize"/> value 
		/// is unapplicable, then the value will be ignored.
		/// </remarks>
		public void UpdateTable(DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			CheckAllowCreate(tableInfo.TableName);
			CommittableTransaction.AlterTable(tableInfo.TableName, tableInfo, dataSectorSize, indexSectorSize);
		}

		/// <summary>
		/// If a table exists with the given table name (defined by <paramref name="tableInfo"/>)
		/// alters its the structure, otherwise creates a new table.
		/// </summary>
		/// <param name="tableInfo">Meta informations for altering or creating a table.</param>
		/// <param name="dataSectorSize">Size of data sectors of the table.</param>
		/// <param name="indexSectorSize">Size of the index sectors of the table.</param>
		/// <remarks>
		/// This should only be used as very fine grain optimization for creating or
		/// altering tables.
		/// If in the future the underlying table model is changed so that the given 
		/// <paramref name="dataSectorSize"/> and <paramref name="indexSectorSize"/> 
		/// values are unapplicable and will be ignored.
		/// </remarks>
		public void AlterCreateTable(DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			if (!TableExists(tableInfo.TableName)) {
				CreateTable(tableInfo, dataSectorSize, indexSectorSize);
			} else {
				UpdateTable(tableInfo, dataSectorSize, indexSectorSize);
			}
		}

		/// <summary>
		/// If a table exists with the given table name (defined by <paramref name="tableInfo"/>)
		/// alters its the structure, otherwise creates a new table.
		/// </summary>
		/// <param name="tableInfo">Meta informations for altering or creating a table.</param>
		/// <exception cref="StatementException"></exception>
		public void AlterCreateTable(DataTableInfo tableInfo) {
			if (!TableExists(tableInfo.TableName)) {
				CreateTable(tableInfo);
			} else {
				UpdateTable(tableInfo);
			}
		}

		/// <summary>
		/// Drops a table within the transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to drop.</param>
		/// <exception cref="TransactionException">
		/// If none tables was found for the given <paramref name="tableName"/>.
		/// </exception>
		public void DropTable(string tableName) {
			DropTable(new TableName(currentSchema, tableName));
		}

		/// <summary>
		/// Drops a table within the transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to drop.</param>
		/// <exception cref="TransactionException">
		/// If none tables was found for the given <paramref name="tableName"/>.
		/// </exception>
		public void DropTable(TableName tableName) {
			CommittableTransaction.DropTable(tableName);
		}

		/// <summary>
		/// Compacts a table with the given name.
		/// </summary>
		/// <param name="tableName">The name of the table to compact.</param>
		/// <exception cref="StatementException">
		/// If none table was found for the given <paramref name="tableName"/> 
		/// in the <see cref="CurrentSchema"/>.
		/// </exception>
		public void CompactTable(string tableName) {
			CompactTable(new TableName(currentSchema, tableName));
		}

		/// <summary>
		/// Compacts a table with the given name.
		/// </summary>
		/// <param name="tableName">The name of the table to compact.</param>
		/// <exception cref="StatementException">
		/// If none table was found for the given <paramref name="tableName"/>.
		/// </exception>
		public void CompactTable(TableName tableName) {
			CommittableTransaction.CompactTable(tableName);
		}

		///<summary>
		/// Adds the given table name to the list of tables that are selected from
		/// within the transaction in this connection.
		///</summary>
		///<param name="tableName"></param>
		public void AddSelectedFromTable(string tableName) {
			AddSelectedFromTable(new TableName(currentSchema, tableName));
		}

		///<summary>
		/// Adds the given table name to the list of tables that are selected from
		/// within the transaction in this connection.
		///</summary>
		///<param name="name"></param>
		public void AddSelectedFromTable(TableName name) {
			CommittableTransaction.AddSelectedFromTable(name);
		}

		#endregion

		#region Constraints

		/// <summary>
		/// Checks all the rows in the table for immediate constraint violations
		/// and when the transaction is next committed check for all deferred
		/// constraint violations.
		/// </summary>
		/// <param name="tableName">Name of the table to check the constraints.</param>
		/// <remarks>
		/// This method is used when the constraints on a table changes and we 
		/// need to determine if any constraint violations occurred.
		/// <para>
		/// To the constraint checking system, this is like adding all the 
		/// rows to the given table.
		/// </para>
		/// </remarks>
		/// <exception cref="StatementException">
		/// If none table with the given <paramref name="tableName"/> was found.
		/// </exception>
		public void CheckAllConstraints(TableName tableName) {
			// Assert
			CheckExclusive();
			CommittableTransaction.CheckAllConstraints(tableName);
		}

		public void AddUniqueConstraint(TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			// Assert
			CheckExclusive();
			CommittableTransaction.AddUniqueConstraint(tableName, columns, deferred, constraintName);
		}

		public void AddForeignKeyConstraint(TableName table, string[] columns,
			TableName refTable, string[] refColumns,
			ConstraintAction deleteRule, ConstraintAction updateRule,
			ConstraintDeferrability deferred, string constraintName) {
			// Assert
			CheckExclusive();
			CommittableTransaction.AddForeignKeyConstraint(table, columns, refTable, refColumns, deleteRule, updateRule, deferred, constraintName);
		}

		public void AddPrimaryKeyConstraint(TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			// Assert
			CheckExclusive();
			CommittableTransaction.AddPrimaryKeyConstraint(tableName, columns, deferred, constraintName);
		}

		public void AddCheckConstraint(TableName tableName, Expression expression, ConstraintDeferrability deferred, String constraintName) {
			// Assert
			CheckExclusive();
			CommittableTransaction.AddCheckConstraint(tableName, expression, deferred, constraintName);
		}

		public void DropAllConstraintsForTable(TableName tableName) {
			// Assert
			CheckExclusive();
			CommittableTransaction.DropAllConstraintsForTable(tableName);
		}

		public int DropNamedConstraint(TableName tableName, string constraintName) {
			// Assert
			CheckExclusive();
			return CommittableTransaction.DropNamedConstraint(tableName, constraintName);
		}

		public bool DropPrimaryKeyConstraintForTable(TableName tableName, string constraintName) {
			// Assert
			CheckExclusive();
			return CommittableTransaction.DropPrimaryKeyConstraintForTable(tableName, constraintName);
		}

		public TableName[] QueryTablesRelationallyLinkedTo(TableName table) {
			return Transaction.QueryTablesRelationallyLinkedTo(table);
		}

		public DataConstraintInfo[] QueryTableUniqueGroups(TableName tableName) {
			return Transaction.QueryTableUniques(tableName);
		}

		public DataConstraintInfo QueryTablePrimaryKeyGroup(TableName tableName) {
			return Transaction.QueryTablePrimaryKey(tableName);
		}

		public DataConstraintInfo[] QueryTableCheckExpressions(TableName tableName) {
			return Transaction.QueryTableCheckExpressions(tableName);
		}

		public DataConstraintInfo[] QueryTableForeignKeyReferences(TableName tableName) {
			return Transaction.QueryTableForeignKeys(tableName);
		}

		public DataConstraintInfo[] QueryTableImportedForeignKeyReferences(TableName tableName) {
			return Transaction.QueryTableImportedForeignKeys(tableName);
		}

		#endregion

		#region Views

		/// <summary>
		/// The connection view manager that handles view information through this
		/// connection.
		/// </summary>
		private ViewManager viewManager;

		/// <summary>
		/// Creates a new view.
		/// </summary>
		/// <param name="query"></param>
		/// <param name="view">View meta informations used to create the view.</param>
		/// <remarks>
		/// Note that this is a transactional operation. You need to commit for 
		/// the view to be visible to other transactions.
		/// </remarks>
		/// <exception cref="DatabaseException"/>
		public void CreateView(SqlQuery query, View view) {
			CheckAllowCreate(view.TableInfo.TableName);

			try {
				viewManager.DefineView(view, query, User);
			} catch (DatabaseException e) {
				Logger.Error(this, e);
				throw new Exception("Database Exception: " + e.Message, e);
			}

		}

		/// <summary>
		/// Drops a view with the given name.
		/// </summary>
		/// <param name="viewName">Name of the view to drop.</param>
		/// <remarks>
		/// Note that this is a transactional operation. You need to commit 
		/// for the change to be visible to other transactions.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the drop succeeded, otherwise <b>false</b> if 
		/// the view was not found.
		/// </returns>
		public bool DropView(TableName viewName) {
			try {
				return viewManager.DeleteView(viewName);
			} catch (DatabaseException e) {
				Logger.Error(this, e);
				throw new Exception("Database Exception: " + e.Message, e);
			}

		}

		#endregion

		#region Sequences

		/// <summary>
		/// Requests the sequence generator for the next value.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <returns></returns>
		public long NextSequenceValue(String name) {
			// Resolve and ambiguity test
			TableName seq_name = ResolveToTableName(name);
			return Transaction.NextSequenceValue(seq_name);
		}

		/// <summary>
		/// Returns the current sequence value for the given sequence generator.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// The value returned is the same value returned by <see cref="NextSequenceValue"/>.
		/// <para>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If no value was returned by <see cref="NextSequenceValue"/>.
		/// </exception>
		public long LastSequenceValue(String name) {
			// Resolve and ambiguity test
			TableName seq_name = ResolveToTableName(name);
			return Transaction.LastSequenceValue(seq_name);
		}

		/// <summary>
		/// Sets the sequence value for the given sequence generator.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the generator does not exist or it is not possible to set the 
		/// value for the generator.
		/// </exception>
		public void SetSequenceValue(String name, long value) {
			// Resolve and ambiguity test
			TableName seqName = ResolveToTableName(name);
			Transaction.SetSequenceValue(seqName, value);
		}

		/// <summary>
		/// Returns the next unique identifier for the given table from 
		/// the schema.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public long NextUniqueId(TableName name) {
			return Transaction.NextUniqueId(name);
		}

		/// <summary>
		/// Returns the next unique identifier for the given table from 
		/// the current schema.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns></returns>
		public long NextUniqueId(String tableName) {
			TableName tname = TableName.Resolve(currentSchema, tableName);
			return NextUniqueId(tname);
		}

		/// <summary>
		/// Returns the current unique identifier for the given table from
		/// the current schema.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns></returns>
		public long CurrentUniqueId(TableName tableName) {
			return Transaction.CurrentUniqueId(tableName);
		}

		public long CurrentUniqueId(string tableName) {
			return CurrentUniqueId(TableName.Resolve(currentSchema, tableName));
		}

		/// <summary>
		/// Creates a new sequence generator with the given name and initializes 
		/// it with the given details.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="startValue"></param>
		/// <param name="incrementBy"></param>
		/// <param name="minValue"></param>
		/// <param name="maxValue"></param>
		/// <param name="cache"></param>
		/// <param name="cycle"></param>
		/// <remarks>
		/// This does <b>not</b> check if the given name clashes with an 
		/// existing database object.
		/// </remarks>
		public void CreateSequenceGenerator(TableName name, long startValue, long incrementBy, long minValue, long maxValue, long cache, bool cycle) {

			// Check the name of the database object isn't reserved (OLD/NEW)
			CheckAllowCreate(name);

			CommittableTransaction.CreateSequenceGenerator(name, startValue, incrementBy, minValue, maxValue, cache, cycle);
		}

		/// <summary>
		/// Drops an existing sequence generator with the given name.
		/// </summary>
		/// <param name="name"></param>
		public void DropSequenceGenerator(TableName name) {
			CommittableTransaction.DropSequenceGenerator(name);
		}

		#endregion

		#region Variables

		/// <summary>
		/// Assigns a variable to the expression for the session.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="exp"></param>
		/// <param name="context">A context used to evaluate the expression
		/// forming the value of the variable.</param>
		/// <remarks>
		/// This is a generic way of setting properties of the session.
		/// <para>
		/// Special variables, that are recalled by the system, are:
		/// <list type="bullet">
		/// <item><c>ERROR_ON_DIRTY_SELECT</c>: set to <b>true</b> for turning 
		/// the transaction conflict off on the session.</item>
		/// <item><c>CASE_INSENSITIVE_IDENTIFIERS</c>: <b>true</b> means the grammar 
		/// becomes case insensitive for identifiers resolved by the 
		/// grammar.</item>
		/// </list>
		/// </para>
		/// </remarks>
		public void SetVariable(string name, Expression exp, IQueryContext context) {
			if (name.ToUpper().Equals("ERROR_ON_DIRTY_SELECT")) {
				errorOnDirtySelect = ToBooleanValue(exp, context);
			} else if (name.ToUpper().Equals("CASE_INSENSITIVE_IDENTIFIERS")) {
				IsInCaseInsensitiveMode = ToBooleanValue(exp, context);
			} else {
				Transaction.Variables.SetVariable(name, exp, context);
			}
		}

		public Variable DeclareVariable(string name, TType type, bool constant, bool notNull) {
			return Transaction.Variables.DeclareVariable(name, type, constant, notNull);
		}

		public Variable DeclareVariable(string name, TType type, bool notNull) {
			return DeclareVariable(name, type, false, notNull);
		}

		public Variable DeclareVariable(string name, TType type) {
			return DeclareVariable(name, type, false);
		}

		public Variable GetVariable(string name) {
			return Transaction.Variables.GetVariable(name);
		}

		public void RemoveVariable(string name) {
			Transaction.Variables.RemoveVariable(name);
		}

		/// <inheritdoc cref="Transactions.Transaction.SetPersistentVariable"/>
		public void SetPersistentVariable(string variable, String value) {
			// Assert
			CheckExclusive();
			CommittableTransaction.SetPersistentVariable(variable, value);
		}

		/// <inheritdoc cref="Transactions.Transaction.GetPersistantVariable"/>
		public String GetPersistentVariable(string variable) {
			return Transaction.GetPersistantVariable(variable);
		}

		 #endregion

		#region Cursors

		/// <summary>
		/// Declares a cursor identified by the given name and on
		/// the specified query.
		/// </summary>
		/// <param name="name">The name of the cursor to create.</param>
		/// <param name="queryPlan">The query used by the cursor to iterate
		/// through the results.</param>
		/// <param name="attributes">The attributes to define a cursor.</param>
		/// <returns>
		/// Returns the newly created <see cref="Cursor"/> instance.
		/// </returns>
		public Cursor DeclareCursor(TableName name, IQueryPlanNode queryPlan, CursorAttributes attributes) {
			return CommittableTransaction.DeclareCursor(name, queryPlan, attributes);
		}

		/// <summary>
		/// Declares a scrollable cursor identified by the given name and on
		/// the specified query.
		/// </summary>
		/// <param name="name">The name of the cursor to create.</param>
		/// <param name="queryPlan">The query used by the cursor to iterate
		/// through the results.</param>
		/// <returns>
		/// Returns the newly created <see cref="Cursor"/> instance.
		/// </returns>
		public Cursor DeclareCursor(TableName name, IQueryPlanNode queryPlan) {
			return DeclareCursor(name, queryPlan, CursorAttributes.ReadOnly);
		}

		/// <summary>
		/// Gets the instance of a cursor name.
		/// </summary>
		/// <param name="name">The name of the cursor to get.</param>
		/// <returns>
		/// Returns the instance of the <see cref="Cursor"/> identified by
		/// the given name, or <c>null</c> if it was not found.
		/// </returns>
		public Cursor GetCursor(TableName name) {
			return Transaction.GetCursor(name);
		}

		public bool CursorExists(TableName name) {
			return Transaction.CursorExists(name);
		} 

		#endregion

		#region Routines

		/// <summary>
		/// The procedure manager object for this connection.
		/// </summary>
		private RoutinesManager routinesManager;

		/// <summary>
		/// Returns the RoutinesManager object that manages database functions and
		/// procedures in the database for this connection/user.
		/// </summary>
		public RoutinesManager RoutinesManager {
			get { return routinesManager; }
		}

		/// <summary>
		/// Creates an object that implements <see cref="IProcedureConnection"/> 
		/// that provides access to this connection.
		/// </summary>
		/// <param name="user"></param>
		/// <remarks>
		/// Note that this session is set to the user of the privileges that the
		/// procedure executes under when this method returns.
		/// <para>
		/// There must be a 100% guarentee that after this method is called, a call to
		/// <see cref="DisposeProcedureConnection"/> is called which cleans up the state of this
		/// object.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal IProcedureConnection CreateProcedureConnection(User user) {
			// Create the IProcedureConnection object,
			DCProcedureConnection c = new DCProcedureConnection(this);
			// Record the current user
			c.previous_user = User;
			// Record the current 'close_transaction_disabled' flag
			c.transaction_disabled_flag = closeTransactionDisabled;
			// Set the new user
			User = user;
			// Disable the ability to close a transaction
			closeTransactionDisabled = true;
			// Return
			return c;
		}

		/// <summary>
		/// Disposes a <see cref="IProcedureConnection"/> previously created 
		/// by <see cref="CreateProcedureConnection"/>.
		/// </summary>
		/// <param name="connection"></param>
		internal void DisposeProcedureConnection(IProcedureConnection connection) {
			DCProcedureConnection c = (DCProcedureConnection)connection;
			// Revert back to the previous user.
			User = c.previous_user;
			// Revert back to the previous transaction disable status.
			closeTransactionDisabled = c.transaction_disabled_flag;
			// Dispose of the connection
			c.dispose();
		}

		/// <summary>
		/// An implementation of <see cref="IProcedureConnection"/> generated from 
		/// this object.
		/// </summary>
		private class DCProcedureConnection : IProcedureConnection {
			private readonly DatabaseConnection conn;
			/// <summary>
			/// The User of this connection before this procedure was started.
			/// </summary>
			internal User previous_user;

			/// <summary>
			/// The 'close_transaction_disabled' flag when this connection was created.
			/// </summary>
			internal bool transaction_disabled_flag;

			/// <summary>
			/// The ADO.NET connection created by this object.
			/// </summary>
			private IDbConnection db_connection;

			public DCProcedureConnection(DatabaseConnection conn) {
				this.conn = conn;
			}


			public IDbConnection GetDbConnection() {
				if (db_connection == null) {
					db_connection = InternalDbHelper.CreateDbConnection(conn.User, conn);
				}
				return db_connection;
			}

			public Database Database {
				get { return conn.Database; }
			}


			internal void dispose() {
				previous_user = null;
				if (db_connection != null) {
					try {
						InternalDbHelper.DisposeDbConnection(db_connection);
					} catch (Exception e) {
						conn.Logger.Error(this, "Error disposing internal connection.");
						conn.Logger.Error(this, e);
						// We don't wrap this exception
					}
				}
			}
		}

		#endregion

		#region Triggers

		/// <summary>
		/// A buffer of triggers.  This contains triggers that can't fire until
		/// the current transaction has closed.  These triggers were generated by
		/// external actions outside of the context of this transaction.
		/// </summary>
		private readonly List<TriggerEventArgs> triggerEventBuffer;

		/// <summary>
		/// A list of triggers that are fired by actions taken on tables in this
		/// transaction.  When the transaction is successfully committed, these
		/// trigger events need to be propogated to other connections in the database
		/// listening for trigger events on the triggered objects.
		/// </summary>
		private readonly List<TriggerEventArgs> triggerEventList;

		/// <summary>
		/// The connection trigger manager that handles actions that cause triggers
		/// to fire on this connection.
		/// </summary>
		private ConnectionTriggerManager triggerManager;

		/// <summary>
		/// A local member that represents the OLD and NEW system tables that
		/// represent the OLD and NEW data in a triggered action.
		/// </summary>
		private readonly OldAndNewTableContainer oldNewContainer;

		/// <summary>
		/// The current state of the OLD and NEW system tables including any cached
		/// information about the tables.
		/// </summary>
		private OldNewTableState currentOldNewState = new OldNewTableState();

		/// <summary>
		/// Returns the connection trigger manager for this connection.
		/// </summary>
		public ConnectionTriggerManager TriggerManager {
			get { return triggerManager; }
		}

		///<summary>
		/// Adds a type of trigger for the given trigger source (usually the
		/// name of the table).
		///</summary>
		///<param name="triggerName"></param>
		///<param name="triggerSource"></param>
		///<param name="type"></param>
		/// <remarks>
		/// Adds a type of trigger to the given Table.  When the event is fired, the
		/// <see cref="TriggerCallback"/> delegate is notified of the event.
		/// </remarks>
		public void CreateCallbackTrigger(string triggerName, TableName triggerSource, TriggerEventType type) {
			TriggerManager.CreateCallbackTrigger(triggerName, type, triggerSource, FireCallbackTrigger);
		}

		/// <summary>
		/// Removes a type of trigger for the given trigger source (usually the
		/// name of the table).
		/// </summary>
		/// <param name="triggerName"></param>
		public void DeleteCallbackTrigger(string triggerName) {
			TriggerManager.DropCallbackTrigger(triggerName);
		}

		/// <summary>
		/// Informs the underlying transaction that a high level transaction event
		/// has occurred and should be dispatched to any listeners occordingly.
		/// </summary>
		/// <param name="args"></param>
		public void OnTriggerEvent(TriggerEventArgs args) {
			triggerEventList.Add(args);
		}

		/// <summary>
		/// Notifies the session that an insert/delete or update operation has occurred 
		/// on some table of this <see cref="DatabaseConnection"/>.
		/// </summary>
		/// <param name="args"></param>
		/// <remarks>
		/// This should notify the trigger connection manager of this event so that it 
		/// may perform any action that may have been set up to occur on this event.
		/// </remarks>
		internal void FireTableEvent(TriggerEventArgs args) {
			triggerManager.PerformTriggerAction(args);
		}

		private void FireCallbackTrigger(object sender, TriggerEventArgs args) {
			try {
				// Did we pass in a call back interface?
				if (triggerCallback != null) {
					lock (triggerEventBuffer) {
						// If there is no active transaction then fire trigger immediately.
						if (transaction == null) {
							triggerCallback(args.TriggerName, args.Source.ToString(), args.EventType, args.FireCount);
						}
							// Otherwise add to buffer
						else {
							triggerEventBuffer.Add(args);
						}
					}
				}
			} catch (Exception e) {
				Logger.Error(this, "TRIGGER Exception: " + e.Message);
			}

		}


		/// <summary>
		/// Fires any triggers that are pending in the trigger buffer.
		/// </summary>
		private void FirePendingTriggerEvents() {
			int sz;
			lock (triggerEventBuffer) {
				sz = triggerEventBuffer.Count;
			}
			if (sz > 0) {
				// Post an event that fires the triggers for each listener.
				// Post the event to go off approx 3ms from now.
				Database.Context.PostEvent(3, Database.Context.CreateEvent(delegate {
					lock (triggerEventBuffer) {
						// Fire all pending trigger events in buffer
						foreach (TriggerEventArgs args in triggerEventBuffer) {
							triggerCallback(args.TriggerName, args.Source.ToString(), args.EventType, args.FireCount);
						}
						// Clear the buffer
						triggerEventBuffer.Clear();
					}

				}));
			}

		}

		// ---------- Triggered OLD/NEW table handling ----------
		// These methods are used by the triggerManager object to
		// temporarily create OLD and NEW tables in this connection from inside a
		// triggered action.  In some cases (before the operation) the OLD table
		// is mutable.

		/// <summary>
		/// Returns the current state of the old/new tables.
		/// </summary>
		/// <returns></returns>
		internal OldNewTableState GetOldNewTableState() {
			return currentOldNewState;
		}

		/**
		 * Sets the current state of the old/new tables.  When nesting OLD/NEW
		 * tables for nested stored procedures, the current state should be first
		 * recorded and reverted back when the nested procedure finishes.
		 */
		internal void SetOldNewTableState(OldNewTableState state) {
			currentOldNewState = state;
		}

		/// <summary>
		/// An internal table info object that handles OLD and NEW tables for
		/// triggered actions.
		/// </summary>
		private class OldAndNewTableContainer : IInternalTableContainer {
			private readonly DatabaseConnection conn;

			public OldAndNewTableContainer(DatabaseConnection conn) {
				this.conn = conn;
			}

			private bool HasOLDTable {
				get { return conn.currentOldNewState.OldRowIndex != -1; }
			}

			private bool HasNEWTable {
				get { return conn.currentOldNewState.NewDataRow != null; }
			}

			public int TableCount {
				get {
					int count = 0;
					if (HasOLDTable) {
						++count;
					}
					if (HasNEWTable) {
						++count;
					}
					return count;
				}
			}

			public int FindTableName(TableName name) {
				if (HasOLDTable && name.Equals(SystemSchema.OldTriggerTable)) {
					return 0;
				}
				if (HasNEWTable && name.Equals(SystemSchema.NewTriggerTable)) {
					return HasOLDTable ? 1 : 0;
				}
				return -1;
			}

			public TableName GetTableName(int i) {
				if (HasOLDTable) {
					if (i == 0) {
						return SystemSchema.OldTriggerTable;
					}
				}
				return SystemSchema.NewTriggerTable;
			}

			public bool ContainsTable(TableName name) {
				return FindTableName(name) != -1;
			}

			public String GetTableType(int i) {
				return "SYSTEM TABLE";
			}

			public DataTableInfo GetTableInfo(int i) {
				DataTableInfo tableInfo = conn.GetTableInfo(conn.currentOldNewState.TableSource);
				DataTableInfo newTableInfo = tableInfo.Clone(GetTableName(i));
				return newTableInfo;
			}

			public ITableDataSource CreateInternalTable(int index) {
				DataTableInfo tableInfo = GetTableInfo(index);

				TriggeredOldNewDataSource table = new TriggeredOldNewDataSource(conn.Context, tableInfo);

				if (HasOLDTable) {
					if (index == 0) {

						// Copy data from the table to the new table
						DataTable dtable = conn.GetTable(conn.currentOldNewState.TableSource);
						DataRow oldRow = new DataRow(table);
						int rowIndex = conn.currentOldNewState.OldRowIndex;
						for (int i = 0; i < tableInfo.ColumnCount; ++i) {
							oldRow.SetValue(i, dtable.GetCell(i, rowIndex));
						}
						// All OLD tables are immutable
						table.SetImmutable(true);
						table.SetRowData(oldRow);

						return table;
					}
				}

				table.SetImmutable(!conn.currentOldNewState.IsNewMutable);
				table.SetRowData(conn.currentOldNewState.NewDataRow);

				return table;
			}

		}

		/// <summary>
		/// A IMutableTableDataSource implementation that is used for trigger actions
		/// to represent the data in the OLD and NEW tables.
		/// </summary>
		private sealed class TriggeredOldNewDataSource : GTDataSource, IMutableTableDataSource {
			private readonly DataTableInfo tableInfo;
			private DataRow content;
			private bool immutable;

			public TriggeredOldNewDataSource(SystemContext context, DataTableInfo tableInfo)
				: base(context) {
				this.tableInfo = tableInfo;
			}

			internal void SetImmutable(bool im) {
				immutable = im;
			}

			internal void SetRowData(DataRow dataRow) {
				content = dataRow;
			}

			public override DataTableInfo TableInfo {
				get { return tableInfo; }
			}

			public override int RowCount {
				get { return 1; }
			}

			public override TObject GetCell(int column, int row) {
				if (row < 0 || row > 0) {
					throw new Exception("Row index out of bounds.");
				}
				return content.GetValue(column);
			}

			public int AddRow(DataRow dataRow) {
				throw new Exception("Inserting into table '" + TableInfo.TableName + "' is not permitted.");
			}

			public void RemoveRow(int rowIndex) {
				throw new Exception("Deleting from table '" + TableInfo.TableName + "' is not permitted.");
			}

			public int UpdateRow(int rowIndex, DataRow dataRow) {
				if (immutable)
					throw new Exception("Updating table '" + TableInfo.TableName + "' is not permitted.");
				if (rowIndex < 0 || rowIndex > 0)
					throw new Exception("Row index out of bounds.");

				int sz = TableInfo.ColumnCount;
				for (int i = 0; i < sz; ++i) {
					content.SetValue(i, dataRow.GetValue(i));
				}

				return 0;
			}

			public MasterTableJournal Journal {
				get {
					// Shouldn't be used...
					throw new Exception("Invalid method used.");
				}
			}

			public void FlushIndexChanges() {
				// Shouldn't be used...
				throw new Exception("Invalid method used.");
			}

			public void ConstraintIntegrityCheck() {
				// Should always pass (not integrity check needed for OLD/NEW table.
			}

			public void AddRootLock() {
			}

			public void RemoveRootLock() {
			}
		}

		/// <summary>
		/// An internal table info object that handles OLD and NEW tables for
		/// triggered actions.
		/// </summary>
		internal sealed class OldNewTableState {

			/// <summary>
			///  The name of the table that is the trigger source.
			/// </summary>
			private readonly TableName tableSource;

			/// <summary>
			/// The row index of the OLD data that is being updated or deleted in the
			/// trigger source table.
			/// </summary>
			private readonly int oldRowIndex = -1;

			/// <summary>
			/// The DataRow of the new data that is being inserted/updated in the trigger
			/// source table.
			/// </summary>
			private readonly DataRow newDataRow;

			/// <summary>
			/// If true then the 'new_data' information is mutable which would be true for
			/// a BEFORE trigger.
			/// </summary>
			/// <remarks>
			/// For example, we would want to change the data in the row that caused the 
			/// trigger to fire.
			/// </remarks>
			private readonly bool newMutable;

			/// <summary>
			/// The DataTable object that represents the OLD table, if set.
			/// </summary>
			private DataTable oldDataTable;

			/// <summary>
			/// The DataTable object that represents the NEW table, if set.
			/// </summary>
			private DataTable newDataTable;

			public OldNewTableState(TableName tableSource, int oldRowIndex, DataRow newDataRow, bool newMutable) {
				this.tableSource = tableSource;
				this.oldRowIndex = oldRowIndex;
				this.newDataRow = newDataRow;
				this.newMutable = newMutable;
			}

			internal OldNewTableState() {
			}

			/// <summary>
			///  The name of the table that is the trigger source.
			/// </summary>
			public TableName TableSource {
				get { return tableSource; }
			}

			/// <summary>
			/// The row index of the OLD data that is being updated or deleted in the
			/// trigger source table.
			/// </summary>
			public int OldRowIndex {
				get { return oldRowIndex; }
			}

			/// <summary>
			/// The DataRow of the new data that is being inserted/updated in the trigger
			/// source table.
			/// </summary>
			public DataRow NewDataRow {
				get { return newDataRow; }
			}

			/// <summary>
			/// If true then the 'new_data' information is mutable which would be true for
			/// a BEFORE trigger.
			/// </summary>
			/// <remarks>
			/// For example, we would want to change the data in the row that caused the 
			/// trigger to fire.
			/// </remarks>
			public bool IsNewMutable {
				get { return newMutable; }
			}

			/// <summary>
			/// The DataTable object that represents the OLD table, if set.
			/// </summary>
			public DataTable OldDataTable {
				get { return oldDataTable; }
				set { oldDataTable = value; }
			}

			/// <summary>
			/// The DataTable object that represents the NEW table, if set.
			/// </summary>
			public DataTable NewDataTable {
				get { return newDataTable; }
				set { newDataTable = value; }
			}
		}

		#endregion

		/// <summary>
		/// A list of DataTableInfo system table definitions for tables internal to
		/// the database connection.
		/// </summary>
		private readonly static DataTableInfo[] InternalInfoList;

		static DatabaseConnection() {
			InternalInfoList = new DataTableInfo[5];
			InternalInfoList[0] = SystemSchema.StatisticsTableInfo;
			InternalInfoList[1] = SystemSchema.ConnectionInfoTableInfo;
			InternalInfoList[2] = SystemSchema.CurrentConnectionsTableInfo;
			InternalInfoList[3] = SystemSchema.SqlTypesTableInfo;
			InternalInfoList[4] = SystemSchema.PrivilegesTableInfo;
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
		private class ConnectionInternalTableContainer : InternalTableContainer {
			private readonly DatabaseConnection conn;

			public ConnectionInternalTableContainer(DatabaseConnection conn)
				: base("SYSTEM TABLE", InternalInfoList) {
				this.conn = conn;
			}

			public override ITableDataSource CreateInternalTable(int index) {
				if (index == 0)
					return SystemSchema.GetStatisticsTable(conn);
				if (index == 1)
					return SystemSchema.GetConnectionInfoTable(conn);
				if (index == 2)
					return SystemSchema.GetCurrentConnectionsTable(conn);
				if (index == 3)
					return SystemSchema.GetSqlTypesTable(conn);
				if (index == 4)
					return SystemSchema.GetPrivilegesTable(conn);
				throw new Exception();
			}

		}

		/// <inheritdoc/>
		public void Dispose() {
			Close();
		}
	}
}