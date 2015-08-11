using System;

namespace Deveel.Data.DbSystem {
	class ChildQueryContext : QueryContextBase {
		private IQueryContext parentContext;

		public ChildQueryContext(IQueryContext parentContext) {
			if (parentContext == null)
				throw new ArgumentNullException("parentContext");

			this.parentContext = parentContext;
		}

		public override IQueryContext ParentContext {
			get { return parentContext; }
		}

		public override IUserSession Session {
			get { return ParentContext.Session; }
		}

		protected override void Dispose(bool disposing) {
			parentContext = null;
			base.Dispose(disposing);
		}
	}
}
