using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;
using Deveel.Data.Index;
using Deveel.Data.Spatial;

namespace Deveel.Data.DbSystem {
	public sealed class SystemContext : ISystemContext {
		private Dictionary<string, IDatabaseContext> databases;
 
		public SystemContext()
			: this(DbConfig.Default) {
		}

		public SystemContext(IDbConfig configuration) {
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			Configuration = configuration;
			Init();
		}

		~SystemContext() {
			Dispose(false);
		}

		public IDbConfig Configuration { get; private set; }

		public ISpatialContext SpatialContext { get; private set; }

		public ISearchIndexFactory IndexFactory { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (SpatialContext != null &&
					SpatialContext is IDisposable)
					((IDisposable)SpatialContext).Dispose();

				lock (this) {
					if (databases != null) {
						foreach (var database in databases.Values) {
							database.Dispose();
						}

						databases.Clear();
					}

					databases = null;
				}
			}

			SpatialContext = null;
			IndexFactory = null;
		}

		private void Init() {
			// TODO:
		}

		public IDatabaseContext CreateDatabaseContext(IDbConfig config) {
			if (config == null)
				throw new ArgumentNullException("config");

			var name = config.GetValue(DatabaseConfigKeys.DatabaseName);
			if (name == null)
				throw new ArgumentException(String.Format("The configuration must include the database name."));

			return CreateDatabaseContext(config, name.ToType<string>());
		}

		public IDatabaseContext CreateDatabaseContext(IDbConfig config, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			lock (this) {
				if (databases == null)
					databases = new Dictionary<string, IDatabaseContext>();

				if (databases.ContainsKey(name))
					throw new ArgumentException(String.Format("Database '{0}' already exists in this context.", name));

				var dbConfig = new DbConfig(Configuration);
				config.CopyTo(dbConfig);

				return new DatabaseContext(this, dbConfig);
			}
		}

		public IDatabaseContext GetDatabaseContext(IDbConfig config) {
			if (config == null)
				throw new ArgumentNullException("config");

			var name = config.GetString(DatabaseConfigKeys.DatabaseName);
			return GetDatabaseContext(name);
		}

		public IDatabaseContext GetDatabaseContext(string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			lock (this) {
				IDatabaseContext database;
				if (databases == null ||
					!databases.TryGetValue(name, out database))
					return null;

				return database;
			}
		}
	}
}
