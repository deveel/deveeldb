using System;

namespace Deveel.Data {
	public sealed class SessionAccess : SystemAccess {
		private readonly ISession session;

		public SessionAccess(ISession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			this.session = session;
		}

		protected override ISession Session {
			get { return session; }
		}
	}
}
