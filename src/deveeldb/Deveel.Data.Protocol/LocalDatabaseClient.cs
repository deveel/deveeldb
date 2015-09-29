using System;

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;

namespace Deveel.Data.Protocol {
	class LocalDatabaseClient : IDatabaseClient {
		private bool disposed;
		private int openConnections;

		internal LocalDatabaseClient(LocalClient client, IDatabase database) {
			Database = database;
			Client = client;
		}

		public void Dispose() {
			if (openConnections == 0) {
				Database = null;
				Client = null;
				disposed = true;
			}
		}

		private IDatabase Database { get; set; }

		IClient IDatabaseClient.Client {
			get { return Client; }
		}

		private LocalClient Client { get; set; }

		public IConfiguration Configuration {
			get {
				AssertNotDisposed();
				return Database.DatabaseContext.Configuration;
			}
		}

		private IDatabaseHandler DatabaseHandler {
			get { return Client.SystemContext as IDatabaseHandler; }
		}

		public bool IsBooted {
			get {
				AssertNotDisposed();
				return Database.IsOpen;
			}
		}

		public bool Exist {
			get {
				AssertNotDisposed();
				return Database.Exists;
			}
		}

		private void AssertNotDisposed() {
			if (disposed)
				throw new ObjectDisposedException(GetType().FullName);
		}

		public IServerConnector Create(string adminUser, string adminPassword) {
			AssertNotDisposed();

			if (String.IsNullOrEmpty(adminUser))
				throw new ArgumentNullException("adminUser");
			if (String.IsNullOrEmpty(adminPassword))
				throw new ArgumentNullException("adminPassword");

			Database.Create(adminUser, adminPassword);
			return new ServerConnector(this, DatabaseHandler);
		}

		public IServerConnector Boot() {
			if (IsBooted)
				throw new InvalidOperationException("The local database is already booted.");

			Database.Open();
			return new ServerConnector(this, DatabaseHandler);
		}

		public IServerConnector Access() {
			if (!IsBooted)
				throw new InvalidOperationException("The database is not booted.");

			return new ServerConnector(this, DatabaseHandler);
		}

		#region ServerConnector

		class ServerConnector : EmbeddedServerConnector {
			public LocalDatabaseClient Client { get; set; }

			public ServerConnector(LocalDatabaseClient client, IDatabaseHandler handler) 
				: base(handler) {
				Client = client;
				client.openConnections++;
			}

			protected override void Dispose(bool disposing) {
				if (disposing) {
					Client.openConnections--;
					if (Client.openConnections <= 0) {
						Client.Dispose();
					}
				}

				Client = null;

				base.Dispose(disposing);
			}
		}

		#endregion
	}
}
