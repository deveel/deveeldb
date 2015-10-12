// 
//  Copyright 2010-2015 Deveel
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
using System.Linq;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;
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
	public sealed class Database : IDatabase {
		/// <summary>
		/// Initializes a new instance of the <see cref="Database"/> class providing
		/// a given parent <see cref="IDatabaseContext"/>.
		/// </summary>
		/// <param name="context">The database context that provides the required
		/// configurations and services to the database.</param>
		public Database(IDatabaseContext context) {
			DatabaseContext = context;

			DiscoverDataVersion();

			TableComposite = new TableSourceComposite(this);

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

		/// <summary>
		/// Gets the database name, as configured in the parent context.
		/// </summary>
		/// <value>
		/// The database name.
		/// </value>
		public string Name {
			get { return DatabaseContext.DatabaseName(); }
		}

		/// <summary>
		/// Gets an object that is used to create new transactions to this database
		/// </summary>
		/// <seealso cref="ITransactionFactory" />
		public ITransactionFactory TransactionFactory { get; private set; }

		private void DiscoverDataVersion() {
			var dataVerion = Attribute.GetCustomAttribute(typeof (Database).Assembly, typeof (DataVersionAttribute))
				as DataVersionAttribute;
			if (dataVerion != null)
				Version = dataVerion.Version;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (IsOpen) {
					// TODO: Report the error
				}

				TableComposite.Dispose();
				DatabaseContext.Dispose();
			}

			TableComposite = null;
			DatabaseContext = null;
		}

		/// <summary>
		/// Gets the context that contains this database.
		/// </summary>
		/// <seealso cref="IDatabaseContext" />
		public IDatabaseContext DatabaseContext { get; private set; }

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
		/// <exception cref="System.Exception">An error occurred while testing database existence.</exception>
		/// <seealso cref="Create" />
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

		/// <summary>
		/// Creates the database in the context given, granting the administrative
		/// control to the user identified by the given name and password.
		/// </summary>
		/// <param name="adminName">The name of the administrator.</param>
		/// <param name="adminPassword">The password used to identify the administrator.</param>
		/// <exception cref="DatabaseSystemException">
		/// If the database context is configured to be in read-only model, if it was not possible
		/// to commit the initial information or if another unhanded error occurred while 
		/// creating the database.
		/// </exception>
		/// <exception cref="System.ArgumentNullException">
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
		/// </remarks>
		/// <seealso cref="IDatabaseContext.Configuration" />
		public void Create(string adminName, string adminPassword) {
			if (DatabaseContext.ReadOnly())
				throw new DatabaseSystemException("Cannot create database in read-only mode.");

			if (String.IsNullOrEmpty(adminName))
				throw new ArgumentNullException("adminName");
			if (String.IsNullOrEmpty(adminPassword))
				throw new ArgumentNullException("adminPassword");

			try {
				// Create the conglomerate
				TableComposite.Create();

				using (var session = this.CreateInitialSystemSession()) {
					session.AutoCommit(false);

					using (var context = new SessionQueryContext(session)) {
						session.ExclusiveLock();
						session.CurrentSchema(SystemSchema.Name);

						// Create the schema information tables
						CreateSchemata(context);

						// The system tables that are present in every conglomerate.
						SystemSchema.CreateTables(context);

						// Create the system views
						InformationSchema.CreateViews(context);

						this.CreateAdminUser(context, adminName, adminPassword);

						SetCurrentDataVersion(context);

						// Set all default system procedures.
						// TODO: SystemSchema.SetupSystemFunctions(session, username);

						try {
							// Close and commit this transaction.
							session.Commit();
						} catch (TransactionException e) {
							throw new DatabaseSystemException("Could not commit the initial information", e);
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

		private void SetCurrentDataVersion(IQueryContext context) {
			// TODO: Get the data version and then set it to the database table 'vars'
		}

		private void CreateSchemata(IQueryContext context) {
			try {
				context.CreateSchema(InformationSchema.SchemaName, SchemaTypes.System);
				context.CreateSchema(DatabaseContext.DefaultSchema(), SchemaTypes.Default);
			} catch (DatabaseSystemException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseSystemException("Unable to create the default schema for the database.", ex);
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
					throw new DatabaseSystemException(String.Format("The database {0} does not exist.", this.Name()));

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
				if (DatabaseContext.DeleteOnClose()) {
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

		#region DatabaseTransactionFactory

		class DatabaseTransactionFactory : ITransactionFactory {
			private readonly Database database;

			public DatabaseTransactionFactory(Database database) {
				this.database = database;
				OpenTransactions = new TransactionCollection();
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
		}

		#endregion
	}
}
