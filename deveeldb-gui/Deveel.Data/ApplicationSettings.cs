using System;
using System.Data;

using Deveel.Data.Client;
using Deveel.Data.DbModel;

namespace Deveel.Data {
	public sealed class ApplicationSettings : ISettings {
		public ApplicationSettings() {
			connectionStrings = new DbConnectionStrings();
		}

		private DbConnectionStrings connectionStrings;
		private DbConnectionString connectionString;
		private DeveelDbConnection connection;
		private int untitledDocuments;

		public event EventHandler ConnectionReset;

		public event EventHandler ConnectionStringsChanged;


		public DbConnectionString ConnectionString {
			get { return connectionString; }
			set {
				if (value != connectionString) {
					connectionString = value;
					ResetConnection();
				}
			}
		}

		public DbConnectionStrings ConnectionStrings {
			get { return connectionStrings; }
			set {
				connectionStrings = value;
				OnPastConnectionStringsChanged(EventArgs.Empty);
			}
		}

		public DeveelDbConnection Connection {
			get {
				if (connection == null)
					connection = new DeveelDbConnection(connectionString.ConnectionString);
				return connection;
			}
		}

		private void OnPastConnectionStringsChanged(EventArgs e) {
			if (ConnectionStringsChanged != null) {
				ConnectionStringsChanged(this, e);
			}
		}

		private void OnDatabaseConnectionReset(EventArgs e) {
			if (ConnectionReset != null) {
				ConnectionReset(this, e);
			}
		}

		public void SetProperty(string key, object value) {
			switch (key) {
				case SettingsProperties.ConnectionStringsFile:
					break;
				case SettingsProperties.DateTimeFormat:
					break;
				case SettingsProperties.EnableBatching:
					break;
				case SettingsProperties.LoadPlugins:
					break;
			}
		}

		public object GetProperty(string key) {
			throw new NotImplementedException();
		}

		public void CloseConnection() {
			if (connection == null)
				return;

			if (connection.State != ConnectionState.Closed &&
				connection.State != ConnectionState.Broken)
				connection.Close();
		}

		public void ResetConnection() {
			if (connection != null)
				connection.Dispose();

			connection = null;

			OnDatabaseConnectionReset(EventArgs.Empty);
		}

		public int CountUntitled() {
			return ++untitledDocuments;
		}
	}
}