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
using System.IO;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public sealed class Database : IDatabase {
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

		~Database() {
			Dispose(false);
		}

		public string Name {
			get { return DatabaseContext.DatabaseName(); }
		}

		public ITransactionFactory TransactionFactory { get; private set; }

		void IEventSource.AppendEventData(IEvent @event) {
			// TODO: Is there anything else to add?
			@event.Database(Name);
		}

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

		public IDatabaseContext DatabaseContext { get; private set; }

		public Version Version { get; private set; }

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

		public bool IsOpen { get; private set; }

		internal TableSourceComposite TableComposite { get; private set; }

		public ITable SingleRowTable { get; private set; }

		public void Create(string adminName, string adminPassword) {
			if (DatabaseContext.ReadOnly())
				throw new DatabaseSystemException("Can not create database in Read only mode.");

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
						// TODO: InformationSchema.CreateSystemViews(session);

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
			} catch (DatabaseSystemException e) {
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
			} catch (DatabaseSystemException e) {
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

			public ITransaction CreateTransaction(TransactionIsolation isolation) {
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
