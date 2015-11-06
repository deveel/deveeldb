using System;
using System.Linq;

using Deveel.Data.Mapping;

using IQToolkit;
using IQToolkit.Data;

namespace Deveel.Data.Linq {
	public abstract class QueryContext : IDisposable {
		public QueryContext(IDatabase database) 
			: this(database, new QueryContextSettings()) {
		}

		public QueryContext(IDatabase database, QueryContextSettings settings) {
			Database = database;
			Settings = settings;
		}

		~QueryContext() {
			Dispose(false);
		}

		public IDatabase Database { get; private set; }

		public QueryContextSettings Settings { get; private set; }

		private IQueryProvider Provider { get; set; }

		private DeveelDbProvider CreateProvider() {
			if (Provider == null) {
				var userName = Settings.UserName;
				var password = Settings.Password;

				if (String.IsNullOrEmpty(userName))
					throw new QueryException("The user name is required to connect.");

				if (String.IsNullOrEmpty(password))
					throw new QueryException("The password is required to connect.");

				// TODO: Get all other settings as metadata

				var mappingContext = new MappingContext();
				OnBuildMap(mappingContext);

				var model = mappingContext.CreateModel();
				var providerSettings = new ProviderSettings(userName, password, model);

				Provider = Database.GetQueryProvider(providerSettings);
			}

			return (DeveelDbProvider) Provider;
		}

		protected abstract void OnBuildMap(MappingContext mappingContext);

		internal IEntityTable<T> GetTable<T>() where T : class {
			return CreateProvider().GetTable<T>();
		} 

		public QueryTable<T> Table<T>() where T : class {
			return new QueryTable<T>(this);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected void Dispose(bool disposing) {
			if (disposing) {
				if (Provider != null &&
					Provider is DbEntityProvider)
					((DbEntityProvider)Provider).Connection.Close();
			}

			Provider = null;
		}
	}
}
