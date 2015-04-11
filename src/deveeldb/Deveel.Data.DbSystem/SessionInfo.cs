using System;

using Deveel.Data.Diagnostics;
using Deveel.Data.Protocol;
using Deveel.Data.Security;

namespace Deveel.Data.DbSystem {
	public sealed class SessionInfo {
		public SessionInfo(IUserSession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
			StartedOn = DateTime.UtcNow;
		}

		public IUserSession Session { get; private set; }

		public User User {
			get { return Session.User; }
		}

		public ConnectionEndPoint EndPoint {
			get { return Session.EndPoint; }
		}

		public DateTime StartedOn { get; private set; }

		public DateTime? LastCommandOn { get; private set; }

		public void OnNewEvent(IEvent e) {
			lock (Session) {
				Session.Database.EventRegistry.RegisterEvent(e);
			}
		}
	}
}
