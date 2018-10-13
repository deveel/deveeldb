using System;

using Microsoft.Extensions.DependencyInjection;

namespace Deveel.Data {
	public class Context : IContext {
		private IServiceProvider scope;

		public Context(IContext parent) {
			Parent = parent;

			if (parent == null) {
				scope = new ServiceCollection().BuildServiceProvider();
			} else {
				scope = parent.Scope.CreateScope().ServiceProvider;
			}
		}

		~Context() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {

			}
		}

		IContext IContext.ParentContext => Parent;

		protected IContext Parent { get; }

		string IContext.ContextName => Name;

		protected virtual string Name => "context";

		IServiceProvider IContext.Scope => Scope;

		protected IServiceProvider Scope { get; }
	}
}