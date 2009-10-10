using System;
using System.Collections;

using Deveel.Collections;
using Deveel.Commands;

namespace Deveel.Data.Shell {
	public sealed class SessionManager {

		private static SessionManager _instance;

		private readonly ISortedDictionary /*<String,SQLSession>*/ _sessions;
		private SQLSession _currentSession;

		private SessionManager() {
			_sessions = new TreeMap();
		}

		public static SessionManager getInstance() {
			if (_instance == null)
				_instance = new SessionManager();
			return _instance;
		}

		public void AddSession(String sessionName, SQLSession session) {
			_sessions.Add(sessionName, session);
		}

		public SQLSession RemoveSessionWithName(String sessionName) {
			return (SQLSession)_sessions.Remove(sessionName);
		}

		public SQLSession GetSessionByName(String name) {
			return (SQLSession)_sessions[name];
		}

		public String FirstSessionName {
			get { return (String)_sessions.FirstKey; }
		}

		public bool CloseCurrentSession() {
			_currentSession.Close();
			return RemoveSession(_currentSession);
		}

		private bool RemoveSession(SQLSession session) {
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
			foreach (SQLSession session in _sessions.Values) {
				session.Close();
			}
		}

		public CommandResultCode RenameSession(String oldSessionName, String newSessionName) {
			CommandResultCode result = CommandResultCode.ExecutionFailed;

			if (SessionNameExists(newSessionName)) {
				Console.Error.WriteLine("A session with that name already exists");
			} else {
				SQLSession session = RemoveSessionWithName(oldSessionName);
				if (session != null) {
					AddSession(newSessionName, session);
					_currentSession = session;
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

		public void SetCurrentSession(SQLSession session) {
			this._currentSession = session;
		}

		public SQLSession CurrentSession {
			get { return _currentSession; }
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