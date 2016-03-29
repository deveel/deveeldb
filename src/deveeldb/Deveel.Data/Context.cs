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
