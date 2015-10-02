// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Routines {
	public sealed class FunctionQueryContext : QueryContextBase {
		private IQueryContext parentContext;
		private ExecuteContext executeContext;

		public FunctionQueryContext(IQueryContext parentContext, ExecuteContext executeContext) {
			if (parentContext == null)
				throw new ArgumentNullException("parentContext");
			if (executeContext == null)
				throw new ArgumentNullException("executeContext");

			this.parentContext = parentContext;
			this.executeContext = executeContext;
		}

		public override IUserSession Session {
			get { return parentContext.Session; }
		}

		protected override Variable OnVariableGet(string name) {
			DataObject arg;
			if (executeContext.TryGetArgument(name, out arg))
				return new Variable(new VariableInfo(name, arg.Type, true));
			if (parentContext is IVariableScope)
				return ((IVariableScope) parentContext).OnVariableGet(name);

			return base.OnVariableGet(name);
		}

		protected override void OnVariableDefined(Variable variable) {
			if (executeContext.HasArgument(variable.Name)) {
				VariableManager.DropVariable(variable.Name);
				throw new InvalidOperationException();
			}

			base.OnVariableDefined(variable);
		}

		protected override void OnVariableDropped(Variable variable) {
			// We should never come to this ...
			if (executeContext.HasArgument(variable.Name)) {
				throw new InvalidOperationException();
			}

			base.OnVariableDropped(variable);
		}

		protected override void Dispose(bool disposing) {
			parentContext = null;
			executeContext = null;

			base.Dispose(disposing);
		}
	}
}
