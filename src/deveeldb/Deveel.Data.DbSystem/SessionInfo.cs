using System;

using Deveel.Data.Protocol;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class SessionInfo {
		public SessionInfo(User user) 
			: this(user, TransactionIsolation.Unspecified) {
		}

		public SessionInfo(User user, TransactionIsolation isolation) 
			: this(-1, user, isolation) {
		}

		public SessionInfo(int commitId, User user) 
			: this(commitId, user, TransactionIsolation.Unspecified) {
		}

		public SessionInfo(int commitId, User user, TransactionIsolation isolation) 
			: this(commitId, user, isolation, ConnectionEndPoint.Embedded, null) {
		}

		public SessionInfo(User user, ConnectionEndPoint endPoint, Action<CallbackTriggerEvent> callback) 
			: this(user, TransactionIsolation.Unspecified, endPoint, callback) {
		}

		public SessionInfo(User user, TransactionIsolation isolation, ConnectionEndPoint endPoint) 
			: this(user, isolation, endPoint, null) {
		}

		public SessionInfo(User user, TransactionIsolation isolation, ConnectionEndPoint endPoint, Action<CallbackTriggerEvent> callback) 
			: this(-1, user, isolation, endPoint, callback) {
		}

		public SessionInfo(int commitId, User user, ConnectionEndPoint endPoint) 
			: this(commitId, user, endPoint, null) {
		}

		public SessionInfo(int commitId, User user, ConnectionEndPoint endPoint, Action<CallbackTriggerEvent> callback) 
			: this(commitId, user, TransactionIsolation.Unspecified, endPoint, callback) {
		}

		public SessionInfo(int commitId, User user, TransactionIsolation isolation, ConnectionEndPoint endPoint,
			Action<CallbackTriggerEvent> callback) {
			if (user == null)
				throw new ArgumentNullException("user");
			if (endPoint == null)
				throw new ArgumentNullException("endPoint");

			CommitId = commitId;
			User = user;
			EndPoint = endPoint;
			CallbackTrigger = callback;
			Isolation = isolation;
			StartedOn = DateTimeOffset.UtcNow;
		}

		public int CommitId { get; private set; }

		public ConnectionEndPoint EndPoint { get; private set; }

		public User User { get; private set; }

		public TransactionIsolation Isolation { get; private set; }

		public Action<CallbackTriggerEvent> CallbackTrigger { get; private set; } 

		public DateTimeOffset StartedOn { get; private set; }

		public DateTimeOffset? LastCommandTime { get; private set; }

		// TODO: keep a list of commands issued by the user during the session?

		internal void OnCommand() {
			// TODO: also include the command details?
			LastCommandTime = DateTimeOffset.UtcNow;
		}
	}
}
