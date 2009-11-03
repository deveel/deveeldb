using System;
using System.Collections;

using Deveel.Collections;
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
			SESS_META = new ColumnDesign[5];
			SESS_META[0] = new ColumnDesign("session");
			SESS_META[1] = new ColumnDesign("user");
			SESS_META[2] = new ColumnDesign("conn_string");
			SESS_META[3] = new ColumnDesign("uptime");
			SESS_META[4] = new ColumnDesign("#stmts", ColumnAlignment.Right);
		}

		private bool dirty;
		private readonly TreeMap knownConns;
		private readonly DeveelDBShell application;
		private readonly ConfigurationFile config;

		private readonly static ColumnDesign[] SESS_META;

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
			for (int i = 0; i < SESS_META.Length; ++i) {
				SESS_META[i].ResetWidth();
			}
			TableRenderer table = new TableRenderer(SESS_META, output);
			foreach (string sessionName in application.SessionManager.SessionNames) {
				SqlSession session = application.SessionManager.GetSessionByName(sessionName);
				String prepend = sessionName.Equals(application.CurrentSession.Name) ? " * " : "   ";
				ColumnValue[] row = new ColumnValue[5];
				row[0] = new ColumnValue(prepend + sessionName);
				row[1] = new ColumnValue(session.UserName);
				row[2] = new ColumnValue(session.ConnectionString.ToString());
				row[3] = new ColumnValue(TimeRenderer.RenderTime((long)session.Uptime.TotalMilliseconds));
				row[4] = new ColumnValue(session.StatementCount.ToString());
				table.AddRow(row);
			}
			table.CloseTable();
		}

		public void Save() {
			if (dirty) {
				IDictionary toWrite = new Hashtable();
				foreach (DictionaryEntry entry in knownConns)
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