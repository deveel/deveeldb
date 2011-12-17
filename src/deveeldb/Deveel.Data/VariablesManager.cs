// 
//  Copyright 2010  Deveel
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

using System;
using System.Collections;

namespace Deveel.Data {
	public sealed class VariablesManager {
		internal VariablesManager() {
			variables = new ArrayList();
		}

		private readonly ArrayList variables;

		internal Variable this[int index] {
			get { return variables[index] as Variable; }
		}

		internal int Count {
			get { return variables.Count; }
		}

		public bool SetVariable(string name, Expression exp, IQueryContext context) {
			Variable variable = GetVariable(name);
			if (variable == null)
				return false;

			variable.SetValue(exp, context);
			return true;
		}

		public Variable DeclareVariable(string name, TType type, bool constant, bool notNull) {
			Variable variable = GetVariable(name);
			if (variable != null)
				throw new InvalidOperationException("The variable '" + name + "' was already declared in this session.");

			variable = new Variable(name, type, constant, notNull);
			variables.Add(variable);
			return variable;
		}

		public Variable GetVariable(string name) {
			for (int i = 0; i < variables.Count; i++) {
				Variable variable = variables[i] as Variable;
				if (variable == null)
					continue;

				if (String.Compare(name, variable.Name, false) == 0)
					return variable;
			}

			return null;
		}

		internal void RemoveVariable(string name) {
			for (int i = variables.Count - 1; i >= 0; i--) {
				Variable variable = variables[i] as Variable;
				if (variable == null)
					continue;

				if (String.Compare(name, variable.Name, false) == 0)
					variables.RemoveAt(i);
			}
		}

		internal void Clear() {
			variables.Clear();
		}
	}
}