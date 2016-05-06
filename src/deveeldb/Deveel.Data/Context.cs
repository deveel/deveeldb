// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;

using Deveel.Data.Services;

namespace Deveel.Data {
	/// <summary>
	/// The base implementation of a <see cref="IContext"/> that
	/// defines a scope where to services are stored.
	/// </summary>
	/// <remarks>
	/// This object is convenient for the implementation of other
	/// contexts, since it handles the initialization and disposal
	/// of the <see cref="IScope"/> that it wraps.
	/// </remarks>
	public abstract class Context : IContext {
		private IScope scope;

		/// <summary>
		/// Constructs a new context that has no parent.
		/// </summary>
		protected Context() 
			: this(null) {
		}

		/// <summary>
		/// Constructs a context that is the child of the given other context.
		/// </summary>
		/// <param name="parent">The optional parent context.</param>
		/// <remarks>
		/// The <paramref name="parent"/> context is not required to be <c>not null</c>:
		/// if <c>null</c> then this context will have no parent.
		/// </remarks>
		protected Context(IContext parent) {
			ParentContext = parent;
			InitScope();
		}

		~Context() {
			Dispose(false);
		}

		/// <summary>
		/// When overridden by a derived class, this property returns
		/// a unique name that identifies the context within a global scope.
		/// </summary>
		protected abstract string ContextName { get; }

		/// <summary>
		/// Gets a scope specific for this context, that is used
		/// to resolve services registered within this context
		/// or parent contexts.
		/// </summary>
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
