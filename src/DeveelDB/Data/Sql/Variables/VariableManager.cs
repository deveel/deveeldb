// 
//  Copyright 2010-2018 Deveel
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
using System.Threading.Tasks;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	public sealed class VariableManager : IVariableManager {
		private bool disposed;
		private DbObjectCache<Variable> variables;

		public VariableManager() {
			variables = new DbObjectCache<Variable>();
		}

		~VariableManager() {
			Dispose(false);
		}

		DbObjectType IDbObjectManager.ObjectType => DbObjectType.Variable;

		Task IDbObjectManager.CreateObjectAsync(IDbObjectInfo objInfo) {
			CreateVariable((VariableInfo)objInfo);
			return Task.CompletedTask;
		}

		public void CreateVariable(VariableInfo variableInfo) {
			variables.SetObject(new ObjectName(variableInfo.Name), new Variable(variableInfo));
		}

		Task<bool> IDbObjectManager.RealObjectExistsAsync(ObjectName objName) {
			var value = VariableExists(objName.FullName);
			return Task.FromResult(value);
		}

		Task<bool> IDbObjectManager.ObjectExistsAsync(ObjectName objName) {
			var value = VariableExists(objName.FullName);
			return Task.FromResult(value);
		}

		public bool VariableExists(string name) {
			return variables.ContainsObject(new ObjectName(name));
		}

		Task<IDbObjectInfo> IDbObjectManager.GetObjectInfoAsync(ObjectName objectName) {
			Variable variable;
			if (!variables.TryGetObject(objectName, out variable))
				return Task.FromResult<IDbObjectInfo>(null);

			return Task.FromResult<IDbObjectInfo>(variable.VariableInfo);
		}
			
		Task<IDbObject> IDbObjectManager.GetObjectAsync(ObjectName objName) {
			var result = GetVariable(objName.FullName);
			return Task.FromResult<IDbObject>(result);
		}

		Task<bool> IDbObjectManager.AlterObjectAsync(IDbObjectInfo objInfo) {
			throw new NotSupportedException();
		}

		Task<bool> IDbObjectManager.DropObjectAsync(ObjectName objName) {
			return Task.FromResult(RemoveVariable(objName.FullName));
		}

		Task<ObjectName> IDbObjectManager.ResolveNameAsync(ObjectName objName, bool ignoreCase) {
			ObjectName resolved;
			if (variables.TryResolveName(objName, ignoreCase, out resolved))
				return Task.FromResult(resolved);

			return Task.FromResult<ObjectName>(null);
		}

		public Variable ResolveVariable(string name, bool ignoreCase) {
			ObjectName resolved;
			if (!variables.TryResolveName(new ObjectName(name), ignoreCase, out resolved))
				return null;

			Variable variable;
			if (!variables.TryGetObject(resolved, out variable))
				return null;

			return variable;
		}

		public SqlType ResolveVariableType(string name, bool ignoreCase) {
			Variable variable;
			if (!variables.TryGetObject(new ObjectName(name), out variable))
				return null;

			return variable.Type;
		}

		public Variable GetVariable(string name) {
			Variable variable;
			if (!variables.TryGetObject(new ObjectName(name), out variable))
				return null;

			return variable;
		}

		public SqlExpression AssignVariable(QueryContext context, string name, bool ignoreCase, SqlExpression value) {
			Variable variable;
			if (!variables.TryGetObject(new ObjectName(name), out variable)) {
				var type = value.GetSqlType(context);
				variable = new Variable(name, type);
				variables.SetObject(new ObjectName(name), variable);
			}

			return variable.SetValue(value, context);
		}

		public bool RemoveVariable(string name) {
			return variables.RemoveObject(new ObjectName(name));
		}

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing)
					variables.Dispose();

				variables = null;
				disposed = true;
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}