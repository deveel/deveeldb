using System;
using System.Collections;

using Deveel.Collections;
using Deveel.Data.Client;
using Deveel.Design;
using Deveel.Shell;

namespace Deveel.Data.Shell {
	public sealed class Connections {
		internal Connections(DeveelDBShell application, ConfigurationFile config) {
			this.application = application;
			this.config = config;
			knownConns = new TreeMap(config.Properties);
		}

		static Connections() {
			SessionColumns = new ColumnDesign[5];
			SessionColumns[0] = new ColumnDesign("Name");
			SessionColumns[1] = new ColumnDesign("User");
			SessionColumns[2] = new ColumnDesign("Connection String");
			SessionColumns[3] = new ColumnDesign("Uptime");
			SessionColumns[4] = new ColumnDesign("Statements", ColumnAlignment.Right);
		}

		private bool dirty;
		private readonly TreeMap knownConns;
		private readonly DeveelDBShell application;
		private readonly ConfigurationFile config;

		private readonly static ColumnDesign[] SessionColumns;

		public bool HasConnection(string alias) {
			return knownConns.ContainsKey(alias);
		}

		public string GetConnectionString(string alias) {
			return (string) knownConns[alias];
		}

		public void AddConnectionString(string alias, string connectionString) {
			dirty = true;
			knownConns.Add(alias, connectionString);
		}

		public IEnumerator Complete(string partialWord) {
			return new SortedMatchEnumerator(partialWord, knownConns);
		}


		internal void RenderTable(IOutputDevice output) {
			for (int i = 0; i < SessionColumns.Length; ++i) {
				SessionColumns[i].ResetWidth();
			}
			TableRenderer table = new TableRenderer(SessionColumns, output);
			foreach (DictionaryEntry entry in config.Properties) {
				string sessionName = (string)entry.Key;
				DeveelDbConnectionStringBuilder connString = new DeveelDbConnectionStringBuilder((string)entry.Value);

				SqlSession session = application.SessionManager.GetSessionByName(sessionName);
				String prepend = (session != null && sessionName.Equals(application.CurrentSession.Name) ? " * " : "   ");
				ColumnValue[] row = new ColumnValue[5];
				row[0] = new ColumnValue(prepend + sessionName);
				row[1] = new ColumnValue(session != null ? session.UserName : String.Empty);
				row[2] = new ColumnValue(session != null ? session.ConnectionString.ToString() : connString.ToString());
				row[3] = new ColumnValue(TimeRenderer.RenderTime(session != null ? (long)session.Uptime.TotalMilliseconds : 0L));
				row[4] = new ColumnValue(session != null ? session.StatementCount.ToString() : "0");
				table.AddRow(row);
			}
			table.CloseTable();
		}

		public void Save() {
			if (dirty) {
				IDictionary toWrite = new Hashtable();
				foreach (IMapEntry entry in knownConns)
					toWrite.Add(entry.Key, entry.Value);

				config.ClearValues();
				foreach (DictionaryEntry entry in toWrite)
					config.SetValue((string)entry.Key, (string)entry.Value);
				config.Save("Connections");
				dirty = false;
			}

		}
	}
}