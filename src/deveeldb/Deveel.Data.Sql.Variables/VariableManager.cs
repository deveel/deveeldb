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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Variables {
	public class VariableManager : IObjectManager {
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
				variables.Clear();
			}

			variables = null;
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Variable; }
		}

		public void Create() {
			// TODO:
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			var variableInfo = objInfo as VariableInfo;
			if (variableInfo == null)
				throw new ArgumentException();

			DefineVariable(variableInfo);
		}

		public void DefineVariable(VariableInfo variableInfo) {
			if (variableInfo == null)
				throw new ArgumentNullException("variableInfo");

			if (variables.ContainsKey(variableInfo.VariableName))
				throw new ArgumentException();

			var variable = new Variable(variableInfo);
			variables[variableInfo.VariableName] = variable;
			Scope.OnVariableDefined(variable);
		}

		public bool VariableExists(string name) {
			return variables.ContainsKey(name);
		}

		public Variable GetVariable(string name) {
			Variable variable = Scope.OnVariableGet(name);
			if (variable == null) {
				if (!variables.TryGetValue(name, out variable))
					return null;
			}

			return variable;
		}

		public bool DropVariable(string name) {
			Variable variable;
			if (!variables.TryGetValue(name, out variable))
				return false;

			try {
				Scope.OnVariableDropped(variable);
			} catch (Exception) {
				
			}

			return true;
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return VariableExists(objName.Name);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return VariableExists(objName.Name);
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetVariable(objName.Name);
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			throw new NotSupportedException();
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropVariable(objName.Name);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			throw new NotImplementedException();
		}
	}
}