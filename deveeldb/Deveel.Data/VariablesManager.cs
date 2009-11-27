//  
//  Expression.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

		public void SetVariable(string name, Expression exp, IQueryContext context) {
			Variable variable = GetVariable(name);
			if (variable == null)
				throw new ArgumentException("Variable '" + name + "' was not declared in this session.");

			variable.SetValue(exp, context);
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