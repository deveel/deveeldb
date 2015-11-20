using System;

namespace Deveel.Data.Services {
	public abstract class Context : IContext {
		private IScope scope;

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

		protected virtual IScope ContextScope { 
			get { return scope; } 
		}

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
				scope = ParentContext.Scope.OpenScope(ContextName);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (scope != null)
					scope.Dispose();
			}

			scope = null;
			ParentContext = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}
