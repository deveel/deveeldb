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

		// settings...
		private string connStringFile;
		private string dateTimeFormat;
		private string nullString;
		private bool enableBatches;
		private bool loadPlugins;
		private string pluginFileFilter;

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
					connStringFile = Convert.ToString(value);
					break;
				case SettingsProperties.DateTimeFormat:
					dateTimeFormat = Convert.ToString(value);
					break;
				case SettingsProperties.EnableBatching:
					enableBatches = Convert.ToBoolean(value);
					break;
				case SettingsProperties.LoadPlugins:
					loadPlugins = Convert.ToBoolean(value);
					break;
				case SettingsProperties.NullString:
					nullString = Convert.ToString(value);
					break;
				case SettingsProperties.PluginFileFilter:
					pluginFileFilter = Convert.ToString(value);
					break;
				default:
					throw new ArgumentException();
			}
		}

		public object GetProperty(string key) {
			switch (key) {
				case SettingsProperties.ConnectionStringsFile:
					return connStringFile;
				case SettingsProperties.DateTimeFormat:
					return dateTimeFormat;
				case SettingsProperties.EnableBatching:
					return enableBatches;
				case SettingsProperties.NullString:
					return nullString;
				case SettingsProperties.LoadPlugins:
					return loadPlugins;
				case SettingsProperties.PluginFileFilter:
					return pluginFileFilter;
				default:
					throw new ArgumentException();
			}
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