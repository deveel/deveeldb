using System;

namespace Deveel.Data.DbSystem {
	public class SessionQueryContext : QueryContextBase {
		private IUserSession session;

		public SessionQueryContext(IUserSession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			this.session = session;
		}

		public override IUserSession Session {
			get { return session; }
		}

		protected override void Dispose(bool disposing) {
			session = null;
			base.Dispose(disposing);
		}
	}
}
