using System;

namespace Deveel.Data.DbSystem {
	public sealed class ChildQueryContext : QueryContextBase {
		private IQueryContext parentContext;

		public ChildQueryContext(IQueryContext parentContext) 
			: this(parentContext, null) {
		}

		public ChildQueryContext(IQueryContext parentContext, string label) {
			if (parentContext == null)
				throw new ArgumentNullException("parentContext");

			this.parentContext = parentContext;
			Label = label;
		}

		public override IQueryContext ParentContext {
			get { return parentContext; }
		}

		public string Label { get; private set; }

		public bool IsLabeled {
			get { return !String.IsNullOrEmpty(Label); }
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
