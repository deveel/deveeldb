using System;

namespace Deveel.Data.Services {
	public abstract class Context : IContext {
		protected Context() 
			: this(null) {
		}

		protected Context(IContext parent) {
			ParentContext = parent;
			InitScope();
		}

		~Context() {
			Dispose(false);
		}

		protected abstract string ContextName { get; }

		protected IScope ContextScope { get; private set; }

		protected IContext ParentContext { get; private set; }

		IContext IContext.Parent {
			get { return ParentContext; }
		}

		IScope IContext.Scope {
			get { return ContextScope; }
		}

		string IContext.Name {
			get { return ContextName; }
		}

		private void InitScope() {
			if (ParentContext != null)
				ContextScope = ParentContext.Scope.OpenScope(ContextName);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (ContextScope != null)
					ContextScope.Dispose();
			}

			ContextScope = null;
			ParentContext = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}
