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

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	public sealed class VariableManager : IVariableManager {
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

		private void Dispose(bool disposing) {
			if (disposing) {
				if (variables != null)
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
			variables[variableInfo.VariableName] = variable;
			return variable;
		}

		public bool VariableExists(string name) {
			return variables.ContainsKey(name);
		}

		public Variable GetVariable(string name) {
			Variable variable;
			if (!variables.TryGetValue(name, out variable))
				return null;

			return variable;
		}

		public bool DropVariable(string name) {
			return variables.Remove(name);
		}

		Field IVariableResolver.Resolve(ObjectName variableName) {
			Variable variable;
			if (!variables.TryGetValue(variableName.Name, out variable))
				return null;

			return variable.Value;
		}

		SqlType IVariableResolver.ReturnType(ObjectName variableName) {
			Variable variable;
			if (!variables.TryGetValue(variableName.Name, out variable))
				return null;

			return variable.Type;
		}
	}
}