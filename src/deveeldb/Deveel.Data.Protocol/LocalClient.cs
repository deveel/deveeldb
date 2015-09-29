using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;

namespace Deveel.Data.Protocol {
	public sealed class LocalClient : IClient {
		private Dictionary<string, IDatabaseClient> localClients;
		private bool disposed;

		public LocalClient(ISystemContext systemContext) {
			if (systemContext == null)
				throw new ArgumentNullException("systemContext");

			if (!(systemContext is IDatabaseHandler))
				throw new ArgumentException("The system context does not handle databases");

			SystemContext = systemContext;
			localClients = new Dictionary<string, IDatabaseClient>();
		}

		~LocalClient() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					if (localClients != null) {
						foreach (var client in localClients.Values) {
							client.Dispose();
						}

						localClients.Clear();
					}
				}

				disposed = true;
			}

			localClients = null;
		}

		public IConfiguration Configuration {
			get { return SystemContext.Configuration; }
		}

		public ISystemContext SystemContext { get; private set; }

		public IDatabaseClient ConnectToDatabase(IConfiguration config) {
			var dbConfig = Configuration.Merge(config);
			var databaseName = config.DatabaseName();
			if (String.IsNullOrEmpty(databaseName))
				throw new ArgumentException("The given configuration does not provide any database name.");

			IDatabaseClient client;
			if (!localClients.TryGetValue(databaseName, out client)) {
				var dbHandler = SystemContext as IDatabaseHandler;
				if (dbHandler == null)
					throw new InvalidOperationException("The system context does not handle databases");

				var database = dbHandler.GetDatabase(databaseName);
				if (database == null)
					throw new InvalidOperationException(String.Format("The database '{0}' could not be found in the current context.", databaseName));

				client = new LocalDatabaseClient(this, database);
				localClients[databaseName] = client;
			}

			return client;
		}
	}
}
