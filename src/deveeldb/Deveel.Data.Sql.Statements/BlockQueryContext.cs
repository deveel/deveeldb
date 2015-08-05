using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	class BlockQueryContext : QueryContextBase {
		private IQueryContext parent;

		public BlockQueryContext(IQueryContext parent) {
			if (parent == null)
				throw new ArgumentNullException("parent");

			this.parent = parent;
		}

		public override IQueryContext ParentContext {
			get { return parent; }
		}

		public override IUserSession Session {
			get { return parent.Session; }
		}

		protected override void Dispose(bool disposing) {
			parent = null;
			base.Dispose(disposing);
		}
	}
}
