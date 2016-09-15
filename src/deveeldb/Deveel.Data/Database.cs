// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Security;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// The default implementation of a database in a system.
	/// </summary>
	/// <remarks>
	/// This class implements the <see cref="IDatabase"/> contract,
	/// that is backed by a <see cref="IDatabaseContext"/> for configurations
	/// and services, provides functionalities for the management of data
	/// in the relational model.
	/// </remarks>
	public sealed class Database : EventSource, IDatabase {
		internal Database(DatabaseSystem system, IDatabaseContext context)
			: base(system) {
			System = system;
			Context = context;

			Name = Context.DatabaseName();

			DiscoverDataVersion();

			TableComposite = new TableSourceComposite(this);

			Context.RegisterInstance(this);
			Context.RegisterInstance<ITableSourceComposite>(TableComposite);

			Locker = new Locker(this);

			Sessions = new ActiveSessionList(this);

			Counters = new CounterRegistry();

			Context.RouteImmediate<CounterEvent>(Count);

			// Create the single row table
			var t = new TemporaryTable(context, "SINGLE_ROW_TABLE", new ColumnInfo[0]);
			t.NewRow();
			SingleRowTable = t;

			TransactionFactory = new DatabaseTransactionFactory(this);
		}

		/// <summary>
		/// Finalizes an instance of the <see cref="Database"/> class.
		/// </summary>
		~Database() {
			Dispose(false);
		}

		private void Count(CounterEvent e) {
			if (e.IsIncremental) {
				Counters.Increment(e.CounterKey);
			} else {
				Counters.SetValue(e.CounterKey, e.Value);
			}
		}

		/// <summary>
		/// Gets the database name, as configured in the parent context.
		/// </summary>
		/// <value>
		/// The database name.
		/// </value>
		public string Name { get; private set; }

		public DatabaseSystem System { get; private set; }

		ISystem IDatabase.System {
			get { return System; }
		}

		public CounterRegistry Counters { get; private set; }

		ICounterRegistry IDatabase.Counters {
			get { return Counters; }
		}

		public ActiveSessionList Sessions { get; private set; }

		public Locker Locker { get; private set; }

		/// <summary>
		/// Gets an object that is used to create new transactions to this database
		/// </summary>
		/// <seealso cref="ITransactionFactory" />
		public ITransactionFactory TransactionFactory { get; private set; }

		IContext IContextBased.Context {
			get { return Context; }
		}

		protected override void GetMetadata(Dictionary<string, object> metadata) {
			metadata[MetadataKeys.Database.Name] = Name;
			metadata[MetadataKeys.Database.SessionCount] = Sessions.Count;

			// TODO: add the configurations
		}

		private void DiscoverDataVersion() {
#if PCL
			var dataVerion = typeof(Database).GetTypeInfo().Assembly.GetCustomAttribute<DataVersionAttribute>();
#else
			var dataVerion = Attribute.GetCustomAttribute(typeof (Database).Assembly, typeof (DataVersionAttribute))
				as DataVersionAttribute;
#endif
			if (dataVerion != null)
				Version = dataVerion.Version;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private bool disposed;

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					if (IsOpen) {
						// TODO: Report the error
					}

					if (Locker != null)
						Locker.Dispose();

					if (TableComposite != null)
						TableComposite.Dispose();

					if (TransactionFactory != null &&
					    (TransactionFactory is IDisposable))
						(TransactionFactory as IDisposable).Dispose();

					if (Context != null)
						Context.Dispose();

					if (System != null)
						System.RemoveDatabase(this);
				}

				TransactionFactory = null;
				Locker = null;
				System = null;
				TableComposite = null;
				Context = null;
				disposed = true;
			}
		}

		/// <summary>
		/// Gets the context that contains this database.
		/// </summary>
		/// <seealso cref="IDatabaseContext" />
		public IDatabaseContext Context { get; private set; }

		/// <summary>
		/// Gets the version number of this database.
		/// </summary>
		/// <remarks>
		/// This value is useful for data compatibility between versions
		/// of the system.
		/// </remarks>
		public Version Version { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if the database exists within the
		/// context given.
		/// </summary>
		/// <exception cref="Exception">An error occurred while testing database existence.</exception>
		/// <seealso cref="Create(string,string)" />
		public bool Exists {
			get {
				if (IsOpen)
					//throw new Exception("The database is initialized, so no point testing it's existence.");
					return true;

				try {
					return TableComposite.Exists();
				} catch (IOException e) {
					throw new Exception("An error occurred while testing database existence.", e);
				}
			}
		}

		/// <summary>
		/// Gets a boolean value that indicates if the database was open.
		/// </summary>
		/// <seealso cref="Open" />
		/// <seealso cref="Close" />
		public bool IsOpen { get; private set; }

		internal TableSourceComposite TableComposite { get; private set; }

		/// <summary>
		/// Gets a special table, unique for every database, that has a single
		/// row and a single cell.
		/// </summary>
		public ITable SingleRowTable { get; private set; }

		private void OnDatabaseCreate(IQuery context) {
			var callbacks = context.Context.ResolveAllServices<IDatabaseCreateCallback>();
			if (callbacks != null) {
				foreach (var callback in callbacks) {
					if (callback == null)
						continue;

					try {
						callback.OnDatabaseCreate(context);
					} catch (Exception ex) {
						context.OnError(new Exception(String.Format("Database-Create callback '{0}' caused an error.", callback.GetType()), ex));
					}
				}
			}
		}

		private void OnDatabaseCreated(IQuery context) {
			var callbacks = context.Context.ResolveAllServices<IDatabaseCreatedCallback>();
			if (callbacks != null) {
				foreach (var callback in callbacks) {
					if (callback == null)
						continue;

					try {
						callback.OnDatabaseCreated(context);
					} catch (Exception ex) {
						context.OnError(new Exception(String.Format("Database-Created callback '{0}' caused an error.", callback.GetType()), ex));
					}
				}
			}
		}

		/// <summary>
		/// Creates the database in the context given, granting the administrative
		/// control to the user identified by the given name and token.
		/// </summary>
		/// <param name="adminName">The name of the administrator.</param>
		/// <param name="adminPassword">The password used to identify the administrator</param>
		/// <exception cref="DatabaseSystemException">
		/// If the database context is configured to be in read-only model, if it was not possible
		/// to commit the initial information or if another unhanded error occurred while 
		/// creating the database.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If either one of <paramref name="adminName"/> or <paramref name="adminPassword"/>
		/// are <c>null</c> or empty.
		/// </exception>
		/// <remarks>
		/// <para>
		/// The properties used to create the database are extracted from
		/// the underlying context (<see cref="DatabaseContext" />).
		/// </para>
		/// <para>
		/// This method does not automatically open the database: to make it accessible
		/// a call to <see cref="Open" /> is required.
		/// </para>
		/// <para>
		/// This overload of uses the <see cref="KnownUserIdentifications.ClearText"/> mechanism
		/// to identify the user by the given <paramref name="adminPassword">password</paramref>
		/// </para>
		/// </remarks>
		public void Create(string adminName, string adminPassword) {
			Create(adminName, KnownUserIdentifications.ClearText, adminPassword);
		}

		/// <summary>
		/// Creates the database in the context given, granting the administrative
		/// control to the user identified by the given name and token.
		/// </summary>
		/// <param name="adminName">The name of the administrator.</param>
		/// <param name="token">The toke used to identify the administrator, using the
		/// <paramref name="identification"/> mechanism given.</param>
		/// <param name="identification">The name of the mechanism used to identify the user by the
		/// given token.</param>
		/// <exception cref="DatabaseSystemException">
		/// If the database context is configured to be in read-only model, if it was not possible
		/// to commit the initial information or if another unhanded error occurred while 
		/// creating the database.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If either one of <paramref name="adminName"/> or <paramref name="token"/>
		/// are <c>null</c> or empty.
		/// </exception>
		/// <remarks>
		/// <para>
		/// The properties used to create the database are extracted from
		/// the underlying context (<see cref="DatabaseContext" />).
		/// </para>
		/// <para>
		/// This method does not automatically open the database: to make it accessible
		/// a call to <see cref="Open" /> is required.
		/// </para>
		/// </remarks>
		public void Create(string adminName, string identification, string token) {
			if (Context.ReadOnly())
				throw new DatabaseSystemException("Cannot create database in read-only mode.");

			if (String.IsNullOrEmpty(adminName))
				throw new ArgumentNullException("adminName");
			if (String.IsNullOrEmpty(token))
				throw new ArgumentNullException("token");
			if (String.IsNullOrEmpty(identification))
				throw new ArgumentNullException("identification");

			try {
				// Create the conglomerate
				TableComposite.Create();

				using (var session = this.CreateInitialSystemSession()) {
					using (var query = session.CreateQuery()) {
						try {
							session.CurrentSchema(SystemSchema.Name);

							OnDatabaseCreate(query);

							query.CreateAdminUser(adminName, identification, token);

							OnDatabaseCreated(query);

							try {
								// Close and commit this transaction.
								session.Commit();
							} catch (TransactionException e) {
								throw new DatabaseSystemException("Could not commit the initial information", e);
							}
						} catch (DatabaseSystemException) {
							throw;
						} catch (Exception ex) {
							throw new DatabaseSystemException("An error occurred while creating the database.", ex);
						}
					}
				}

				// Close the conglomerate.
				TableComposite.Close();
			} catch (DatabaseSystemException) {
				throw;
			} catch (Exception e) {
				throw new DatabaseSystemException("An error occurred while creating the database.", e);
			}
		}

		/// <summary>
		/// Opens the database making it ready to be accessed.
		/// </summary>
		/// <exception cref="DatabaseSystemException">
		/// The database was already initialized.
		/// or
		/// or
		/// An error occurred when initializing the database.
		/// </exception>
		/// <remarks>
		/// <para>
		/// This method ensures the system components and the data are
		/// ready to allow any connection to be established.
		/// </para>
		/// <para>
		/// After this method successfully exists, the state of <see cref="IsOpen" />
		/// is changed to <c>true</c>.
		/// </para>
		/// </remarks>
		public void Open() {
			if (IsOpen)
				throw new DatabaseSystemException("The database was already initialized.");

			try {
				// Check if the state file exists.  If it doesn't, we need to report
				// incorrect version.
				if (!TableComposite.Exists())
					// If neither store or state file exist, assume database doesn't
					// exist.
					throw new DatabaseSystemException(String.Format("The database {0} does not exist.", Name));

				// Open the conglomerate
				TableComposite.Open();

				AssertDataVersion();
			} catch (DatabaseSystemException) {
				throw;
			} catch (Exception e) {
				throw new DatabaseSystemException("An error occurred when initializing the database.", e);
			}

			IsOpen = true;
		}

		private void AssertDataVersion() {
			// TODO:
		}

		/// <summary>
		/// Closes the database making it not accessible to connections.
		/// </summary>
		/// <exception cref="DatabaseSystemException">
		/// The database is not initialized.
		/// or
		/// An error occurred during database shutdown.
		/// </exception>
		/// <remarks>
		/// Typical implementations of this interface will automatically
		/// invoke the closure of the database on disposal (<see cref="IDisposable.Dispose" />.
		/// </remarks>
		public void Close() {
			if (!IsOpen)
				throw new DatabaseSystemException("The database is not initialized.");

			try {
				if (Context.DeleteOnClose()) {
					// Delete the tables if the database is set to delete on
					// shutdown.
					TableComposite.Delete();
				} else {
					// Otherwise close the conglomerate.
					TableComposite.Close();
				}
			} catch (DatabaseSystemException) {
				throw;
			} catch (Exception e) {
				throw new DatabaseSystemException("An error occurred during database shutdown.", e);
			} finally {
				IsOpen = false;
			}
		}

		public ILargeObject CreateLargeObject(long objectSize, bool compressed) {
			return TableComposite.CreateLargeObject(objectSize, compressed);
		}

		public ILargeObject GetLargeObject(ObjectId objId) {
			return TableComposite.GetLargeObject(objId);
		}

		public static IDatabase New(IConfiguration config, string adminName, string adminPassword) {
			var dbName = config.GetString("database.name");
			if (String.IsNullOrEmpty(dbName))
				throw new ArgumentException("The database name is not specified in configuration");

			return New(config, null, adminName, adminPassword);
		}

		public static IDatabase New(string dbName, string adminName, string adminPassword) {
			return New(new Configuration.Configuration(), dbName, adminName, adminPassword);
		}

		public static IDatabase New(IConfiguration config, string dbName, string adminName, string adminPassword) {
			if (config == null)
				throw new ArgumentNullException("config");

			if (String.IsNullOrEmpty(dbName))
				throw new ArgumentNullException("dbName");

			config.SetValue("database.name", dbName);

			var builder = new SystemBuilder(config);
			var system = builder.BuildSystem();
			if (!system.DatabaseExists(dbName)) {

				return system.CreateDatabase(config, adminName, adminPassword);
			} else {
				return system.OpenDatabase(config);
			}
		}

#region DatabaseTransactionFactory

		class DatabaseTransactionFactory : ITransactionFactory, IDisposable {
			private Database database;

			public DatabaseTransactionFactory(Database database) {
				this.database = database;
				OpenTransactions = new TransactionCollection();
			}

			~DatabaseTransactionFactory() {
				Dispose(false);
			}

			public TransactionCollection OpenTransactions { get; private set; }

			public ITransaction CreateTransaction(IsolationLevel isolation) {
				lock (this) {
					ITransaction transaction;

					try {
						transaction = database.TableComposite.CreateTransaction(isolation);
					} catch (DatabaseSystemException) {
						throw;
					} catch (Exception ex) {
						throw new DatabaseSystemException("Unable to create a transaction.", ex);
					}

					return transaction;
				}
			}

			private void Dispose(bool disposing) {
				if (disposing) {
					if (OpenTransactions != null) {
						foreach (var transaction in OpenTransactions) {
							if (transaction != null)
								transaction.Dispose();
						}

						OpenTransactions.Clear();
					}
				}

				database = null;
				OpenTransactions = null;
			}

			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}
		}

#endregion
	}
}
