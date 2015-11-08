using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;

namespace Deveel.Data {
	public sealed class SystemManager : IDatabaseHandler, IDisposable {
		private Dictionary<string, IDatabase> databases;
		private IList<IQueryContext> queryContexts; 
		
		public SystemManager() 
			: this(Configuration.Configuration.SystemDefault) {
		}

		public SystemManager(IConfiguration config) 
			: this(new SystemContext(config)) {
		}

		public SystemManager(ISystemContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;
			databases = new Dictionary<string, IDatabase>();

			ScanDatabases();
		}

		private void ScanDatabases() {
			// TODO: if this is a file-based system, scan the system to find them...
		}

		public ISystemContext Context { get; private set; }

		IDatabase IDatabaseHandler.GetDatabase(string databaseName) {
			IDatabase database;
			if (!databases.TryGetValue(databaseName, out database))
				return null;

			return database;
		}

		public void CreateDatabase(string database, string adminUser, string adminPassword) {
			if (String.IsNullOrEmpty(database))
				throw new ArgumentNullException("database");

			lock (this) {
				if (databases.ContainsKey(database))
					throw new InvalidOperationException(string.Format("The database '{0}' already exists in this context.", database));

				var dbContext = new DatabaseContext(Context, database);
				var db = new Database(dbContext);
				if (!db.Exists)
					db.Create(adminUser, adminPassword);

				databases[database] = db;
			}
		}

		public void DeleteDatabase(string database) {
			if (String.IsNullOrEmpty(database))
				throw new ArgumentNullException("database");

			lock (this) {
				IDatabase db;
				if (!databases.TryGetValue(database, out db))
					throw new InvalidOperationException(string.Format("The database '{0}' does not exist in this context", database));

				if (db.Exists)
					db.Close();

				databases.Remove(database);
			}
		}

		public IQueryContext CreateQueryContext(string database, string userName, string password) {
			if (String.IsNullOrEmpty(database))
				throw new ArgumentNullException("database");

			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(password))
				throw new ArgumentNullException("password");

			lock (this) {
				IDatabase db;
				if (!databases.TryGetValue(database, out db))
					throw new ArgumentException(string.Format("The database '{0}' does not exist in this context", database));

				if (!db.IsOpen)
					db.Open();

				var queryContext = db.CreateQueryContext(userName, password);
				if (queryContext == null)
					throw new InvalidOperationException(string.Format("Unable to authenticate user '{0}' on database '{1}'.", userName, database));

				if (queryContexts == null)
					queryContexts = new List<IQueryContext>();

				queryContexts.Add(queryContext);

				return queryContext;
			}
		}


		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				lock (this) {
					if (queryContexts != null) {
						foreach (var queryContext in queryContexts) {
							try {
								if (queryContext != null)
									queryContext.Dispose();
							} catch (Exception) {
								
							}
						}
					}

					if (databases != null) {
						foreach (var db in databases.Values) {
							if (db != null)
								db.Dispose();
						}
					}

					if (Context != null) {
						Context.Dispose();
					}
				}
			}

			lock (this) {
				queryContexts = null;
				databases = null;
				Context = null;
			}
		}
	}
}
