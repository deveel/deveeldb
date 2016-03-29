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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CallStatement  : SqlStatement, IPlSqlStatement {
		public CallStatement(ObjectName procedureName) 
			: this(procedureName, null) {
		}

		public CallStatement(ObjectName procedureName, IEnumerable<SqlExpression> arguments) {
			ProcedureName = procedureName;
			Arguments = arguments;
		}

		private CallStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ProcedureName = (ObjectName) info.GetValue("Procedure", typeof (ObjectName));
			Arguments = (SqlExpression[]) info.GetValue("Arguments", typeof (SqlExpression[]));
		}

		public ObjectName ProcedureName { get; private set; }

		public IEnumerable<SqlExpression> Arguments { get; set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Procedure", ProcedureName);

			var args = new SqlExpression[0];
			if (Arguments != null)
				args = Arguments.ToArray();

			info.AddValue("Arguments", args);
		}

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var args = Arguments;
			if (args != null) {
				var newArgs = new List<SqlExpression>();
				foreach (var arg in args) {
					newArgs.Add(arg.Prepare(preparer));
				}

				args = newArgs.ToArray();
			}

			return new CallStatement(ProcedureName, args);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var procedureName = context.Access.ResolveObjectName(DbObjectType.Routine, ProcedureName);

			return new CallStatement(procedureName, Arguments);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var args = Arguments != null ? Arguments.ToArray() : new SqlExpression[0];
			var invoke = new Invoke(ProcedureName, args);

			var procedure = context.DirectAccess.ResolveProcedure(invoke);
			if (procedure == null)
				throw new StatementException(String.Format("Could not retrieve the procedure '{0}': maybe not a procedure.", ProcedureName));

			if (!context.User.CanExecute(RoutineType.Procedure, invoke,context.Request))
				throw new MissingPrivilegesException(context.User.Name, ProcedureName, Privileges.Execute);

			procedure.Execute(context.Request);
		}
	}
}
