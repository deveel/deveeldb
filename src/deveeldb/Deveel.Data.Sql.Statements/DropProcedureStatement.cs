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
	public sealed class DropProcedureStatement : SqlStatement {
		public DropProcedureStatement(ObjectName procedureName) 
			: this(procedureName, false) {
		}

		public DropProcedureStatement(ObjectName procedureName, bool ifExists) {
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");

			ProcedureName = procedureName;
			IfExists = ifExists;
		}

		public ObjectName ProcedureName { get; private set; }

		public bool IfExists { get; set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var procedureName = context.Access().ResolveObjectName(DbObjectType.Routine, ProcedureName);
			return new DropProcedureStatement(procedureName, IfExists);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.ObjectExists(DbObjectType.Routine, ProcedureName)) {
				if (IfExists)
					return;

				throw new ObjectNotFoundException(ProcedureName);
			}

			if (!context.User.CanDrop(DbObjectType.Routine, ProcedureName))
				throw new MissingPrivilegesException(context.User.Name, ProcedureName, Privileges.Drop);

			var routine = context.DirectAccess.GetObject(DbObjectType.Routine, ProcedureName);
			if (!(routine is IProcedure))
				throw new InvalidOperationException(String.Format("The routine '{0}' is not a procedure.", ProcedureName));

			if (!context.DirectAccess.DropObject(DbObjectType.Routine, ProcedureName))
				throw new InvalidOperationException(String.Format("Unable to drop the procedure '{0}' from the system.", ProcedureName));
		}
	}
}
