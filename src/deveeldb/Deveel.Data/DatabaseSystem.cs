using System;
using System.Collections.Generic;
using System.Net;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Services;

namespace Deveel.Data {
	public sealed class DatabaseSystem : ISystem {
		private IDictionary<string, object> metadata;

		private IDictionary<string, IDatabase> databases; 

		internal DatabaseSystem(ISystemContext context) {
			Context = context;

			CreateMetadata();
		}

		~DatabaseSystem() {
			Dispose(false);
		}

		private void CreateMetadata() {
			metadata = new Dictionary<string, object>();

#if !PCL
			metadata["[env]:os.platform"] = Environment.OSVersion.Platform;
			metadata["[env]:os.version"] = Environment.OSVersion.VersionString;
			metadata["[env]:os.sp"] = Environment.OSVersion.ServicePack;
			metadata["[env]:runtime.version"] = Environment.Version.ToString();
			metadata["[env]:machineName"] = Environment.MachineName;
			metadata["[env]:hostName"] = Dns.GetHostName();

#endif
			metadata["[env]processors"] = Environment.ProcessorCount;

			foreach (var config in Configuration) {
				metadata[String.Format("[config]:{0}", config.Key)] = config.Value;
			}
		}

		public IDatabase GetDatabase(string databaseName) {
			lock (this) {
				if (databases == null)
					return null;

				IDatabase database;
				if (!databases.TryGetValue(databaseName, out database))
					return null;

				return database;
			}
		}

		IEventSource IEventSource.ParentSource {
			get { return null; }
		}

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get { return metadata; }
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				lock (this) {
					if (databases != null) {
						foreach (var database in databases.Values) {
							if (database != null)
								database.Dispose();
						}

						databases.Clear();
					}

					if (Context != null)
						Context.Dispose();
				}
			}

			databases = null;
			Context = null;
		}

		IContext IEventSource.Context {
			get { return Context; }
		}

		public ISystemContext Context { get; private set; }

		private IConfiguration Configuration {
			get { return Context.Configuration; }
		}

		public IEnumerable<string> GetDatabases() {
			lock (this) {
				if (databases == null)
					return new string[0];

				return databases.Keys;
			}
		}

		public IDatabase CreateDatabase(IConfiguration configuration, string adminUser, string adminPassword) {
			lock (this) {
				if (configuration == null)
					throw new ArgumentNullException("configuration");

				var databaseName = configuration.GetString("database.name");
				if (String.IsNullOrEmpty(databaseName))
					throw new ArgumentException("The configuration must specify a database name.");

				if (DatabaseExists(databaseName))
					throw new InvalidOperationException(String.Format("Database '{0}' already exists in the system.", databaseName));

				var dbContext = Context.CreateDatabaseContext(configuration);
				var database = new Database(this, dbContext);

				if (database.Exists)
					throw new InvalidOperationException(String.Format("The database '{0}' was already created.", databaseName));

				database.Create(adminUser, adminPassword);
				database.Open();
				
				if (databases == null)
					databases = new Dictionary<string, IDatabase>();

				databases[databaseName] = database;

				return database;
			}
		}

		public IDatabase OpenDatabase(IConfiguration configuration) {
			lock (this) {
				if (configuration == null)
					throw new ArgumentNullException("configuration");

				var databaseName = configuration.GetString("database.name");
				if (String.IsNullOrEmpty(databaseName))
					throw new ArgumentException("The configuration must specify a database name.");
				
				IDatabase database;
				if (databases == null || 
					!databases.TryGetValue(databaseName, out database))
					throw new InvalidOperationException(String.Format("Database '{0}' does not exist in the system.", databaseName));

				if (!database.IsOpen)
					database.Open();

				return database;
			}
		}

		public bool DatabaseExists(string databaseName) {
			lock (this) {
				if (String.IsNullOrEmpty(databaseName))
					throw new ArgumentNullException("databaseName");

				if (databases == null)
					return false;

				return databases.ContainsKey(databaseName);
			}
		}

		public bool DeleteDatabase(string databaseName) {
			lock (this) {
				if (String.IsNullOrEmpty(databaseName))
					throw new ArgumentNullException("databaseName");

				if (databases == null)
					return false;

				IDatabase database;
				if (!databases.TryGetValue(databaseName, out database))
					return false;

				if (!database.Exists)
					return false;

				// TODO: Implement the delete function in the IDatabase

				return databases.Remove(databaseName);
			}
		}

		internal void RemoveDatabase(Database database) {
			lock (this) {

				databases.Remove(database.Name);
			}
		}
	}
}
