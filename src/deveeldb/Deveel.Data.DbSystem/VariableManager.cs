using System;
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.DbSystem {
	public class VariableManager : IObjectManager, IDisposable {
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
	}
}