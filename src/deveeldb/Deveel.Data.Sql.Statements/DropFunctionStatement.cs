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

using Deveel.Data.Routines;
using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropFunctionStatement : SqlStatement {
		public DropFunctionStatement(ObjectName functionName) 
			: this(functionName, false) {
		}

		public DropFunctionStatement(ObjectName functionName, bool ifExists) {
			if (functionName == null)
				throw new ArgumentNullException("functionName");

			FunctionName = functionName;
			IfExists = ifExists;
		}

		public ObjectName FunctionName { get; private set; }

		public bool IfExists { get; set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var funcionName = context.Access().ResolveObjectName(DbObjectType.Routine, FunctionName);
			return new DropFunctionStatement(funcionName, IfExists);
		}

		protected override void ConfigureSecurity(ExecutionContext context) {
			context.Assertions.AddDrop(FunctionName, DbObjectType.Routine);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.ObjectExists(DbObjectType.Routine, FunctionName)) {
				if (IfExists)
					return;

				throw new ObjectNotFoundException(FunctionName);
			}

			//if (!context.User.CanDrop(DbObjectType.Routine, FunctionName))
			//	throw new MissingPrivilegesException(context.User.Name, FunctionName, Privileges.Drop);

			if (context.DirectAccess.IsSystemFunction(new Invoke(FunctionName), context.Request))
				throw new InvalidOperationException(String.Format("Cannot drop the system function '{0}'.", FunctionName));

			var routine = context.DirectAccess.GetObject(DbObjectType.Routine, FunctionName);
			if (!(routine is IFunction))
				throw new InvalidOperationException(String.Format("The routine '{0}' is not a function.", FunctionName));

			if (!context.DirectAccess.DropObject(DbObjectType.Routine, FunctionName))
				throw new InvalidOperationException(String.Format("Unable to drop the function '{0}' from the system.", FunctionName));
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			string ifExists = IfExists ? "IF EXISTS " : "";
			builder.AppendFormat("DROP FUNCTION {0}{1}", ifExists, FunctionName);
		}
	}
}
