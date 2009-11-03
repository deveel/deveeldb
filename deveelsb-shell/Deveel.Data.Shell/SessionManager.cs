using System;
using System.Collections;
using System.Text;

using Deveel.Collections;
using Deveel.Commands;
using Deveel.Data.Client;

namespace Deveel.Data.Shell {
	public sealed class SessionManager {
		private readonly ISortedDictionary /*<String,SqlSession>*/ _sessions;
		private SqlSession currentSession;

		internal SessionManager() {
			_sessions = new TreeMap();
		}

		private String CreateSessionName(SqlSession session, String name) {
			String userName = null;
			String dbName = null;
			String hostname = null;
			ConnectionString connectionString = session.ConnectionString;

			if (name == null || name.Length == 0) {
				dbName = connectionString.Database;
				StringBuilder result = new StringBuilder();
				userName = session.UserName;
				hostname = connectionString.Host;
				if (userName != null) result.Append(userName + "@");
				if (dbName != null) result.Append(dbName);
				if (dbName != null && hostname != null) result.Append(":");
				if (hostname != null) result.Append(hostname);
				name = result.ToString();
			}
			String key = name;
			int count = 0;
			while (SessionNameExists(key)) {
				++count;
				key = name + "#" + count;
			}
			return key;
		}

		public void AddSession(String sessionName, SqlSession session) {
			_sessions.Add(sessionName, session);
		}

		public string AddSession(SqlSession session, string alias) {
			string sessionName = CreateSessionName(session, alias);
			session.SetName(sessionName);
			AddSession(sessionName, session);
			return sessionName;
		}

		public SqlSession RemoveSessionWithName(String sessionName) {
			return (SqlSession)_sessions.Remove(sessionName);
		}

		public SqlSession GetSessionByName(String name) {
			return (SqlSession)_sessions[name];
		}

		public String FirstSessionName {
			get { return (String)_sessions.FirstKey; }
		}

		public bool CloseCurrentSession() {
			currentSession.Close();
			return RemoveSession(currentSession);
		}

		private bool RemoveSession(SqlSession session) {
			bool result = false;
			IMapEntry entry;
			IIterator it = (IIterator)_sessions.GetEnumerator();
			while (it.MoveNext()) {
				entry = (IMapEntry)it.Current;
				if (entry.Value == session) {
					it.Remove();
					result = true;
					break;
				}
			}

			return result;
		}

		public void CloseAll() {
			foreach (SqlSession session in _sessions.Values) {
				session.Close();
			}
		}

		public CommandResultCode RenameSession(String oldSessionName, String newSessionName) {
			CommandResultCode result = CommandResultCode.ExecutionFailed;

			if (SessionNameExists(newSessionName)) {
				Console.Error.WriteLine("A session with that name already exists");
			} else {
				SqlSession session = RemoveSessionWithName(oldSessionName);
				if (session != null) {
					AddSession(newSessionName, session);
					currentSession = session;
					result = CommandResultCode.Success;
				}
			}

			return result;
		}

		public ISortedSet SessionNames {
			get {
				ISortedSet result = new TreeSet();
				foreach (string key in _sessions.Keys) {
					result.Add(key);
				}
				return result;
			}
		}

		public int SessionCount {
			get { return _sessions.Count; }
		}

		public bool HasSessions {
			get { return _sessions.Count != 0; }
		}

		public bool SessionNameExists(string sessionName) {
			return _sessions.ContainsKey(sessionName);
		}

		public void SetCurrentSession(SqlSession session) {
			this.currentSession = session;
		}

		public SqlSession CurrentSession {
			get { return currentSession; }
		}

		/*  =====================  Helper methods  ======================  */

		/**
		 * Used from several commands that need session name completion.
		 */
		public IEnumerator CompleteSessionName(String partialSession) {
			IEnumerator result = null;
			if (_sessions != null) {
				NameCompleter completer = new NameCompleter(SessionNames);
				// System.out.println("[SessionManager.completeSessionName] created completer for sessionnames "+getSessionNames().toString());
				result = completer.GetAlternatives(partialSession);
			}
			return result;
		}

	}
}