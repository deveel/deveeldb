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
using System.Collections.Generic;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Variables {
	public class VariableManager /*: IResolveCallback*/ {
		private Dictionary<string, Variable> variables;
 
		public VariableManager(IVariableScope scope) {
			Scope = scope;

			variables = new Dictionary<string, Variable>();
		}

		public IVariableScope Scope { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		//void IResolveCallback.OnResolved(IResolveScope scope) {
		//	Scope = scope as IVariableScope;
		//}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				variables.Clear();
			}

			variables = null;
		}

		public Variable DefineVariable(VariableInfo variableInfo) {
			if (variableInfo == null)
				throw new ArgumentNullException("variableInfo");

			if (variables.ContainsKey(variableInfo.VariableName))
				throw new ArgumentException();

			var variable = new Variable(variableInfo);
			OnDefineVariable(variable);
			Scope.OnVariableDefined(variable);

			return variable;
		}

		protected virtual void OnDefineVariable(Variable variable) {
			variables[variable.Name] = variable;
		}

		public virtual bool VariableExists(string name) {
			return variables.ContainsKey(name);
		}

		public Variable GetVariable(string name) {
			Variable variable = Scope.OnVariableGet(name);
			if (variable == null) {
				variable = OnGetVariable(name);
			}

			return variable;
		}

		protected virtual Variable OnGetVariable(string name) {
			Variable variable;
			if (!variables.TryGetValue(name, out variable))
				return null;

			return variable;
		}

		protected virtual bool OnDropVariable(string name, out Variable variable) {
			if (!variables.TryGetValue(name, out variable))
				return false;

			return variables.Remove(name);
		}

		public bool DropVariable(string name) {
			Variable variable;
			if (!OnDropVariable(name, out variable))
				return false;

			try {
				Scope.OnVariableDropped(variable);
			} catch (Exception) {
				
			}

			return true;
		}
	}
}