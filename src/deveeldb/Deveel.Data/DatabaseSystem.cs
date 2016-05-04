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
using System.Net;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// A system of configuration and coordination that manages more databases.
	/// </summary>
	/// <remarks>
	/// Database systems are constructed on top of a given <see cref="ISystemContext"/> instance
	/// that is handling the services, configurations and scope for the system.
	/// <para>
	/// This object handles references to the databases created, opened or existing within
	/// the system: <see cref="Database"/> instances can only be accessed through this object.
	/// </para>
	/// </remarks>
	/// <seealso cref="ISystem"/>
	/// <seealso cref="ISystemContext"/>
	/// <seealso cref="SystemContext"/>
	public sealed class DatabaseSystem : ISystem, IEventSource {
		private IDictionary<string, object> metadata;

		private IDictionary<string, IDatabase> databases; 

		internal DatabaseSystem(ISystemContext context, IEnumerable<ModuleInfo> modules) {
			Context = context;
			Modules = modules;
			OnCreated();
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

			foreach (var module in Modules) {
				metadata[String.Format("[module]:{0}", module.ModuleName)] = module.Version;
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

		public IEnumerable<ModuleInfo> Modules { get; private set; } 
			
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

		private void OnCreated() {
			var callbacks = Context.ResolveAllServices<ISystemCreateCallback>();
			foreach (var callback in callbacks) {
				try {
					callback.OnCreated(this);
				} catch (Exception ex) {
					this.OnError(new Exception(String.Format("Error while disposing: callback of type '{0}'.", callback.GetType()), ex));
				}
			}
		}

		private void OnDispose() {
			var callbacks = Context.ResolveAllServices<ISystemDisposeCallback>();
			foreach (var callback in callbacks) {
				try {
					callback.OnDispose(this);
				} catch (Exception ex) {
					this.OnError(new Exception(String.Format("Error while disposing: callback of type '{0}'.", callback.GetType()), ex));
				}
			}
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

					OnDispose();

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

		public IDatabase CreateDatabase(IConfiguration configuration, string adminUser, string identification, string token) {
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

				database.Create(adminUser, identification, token);
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
