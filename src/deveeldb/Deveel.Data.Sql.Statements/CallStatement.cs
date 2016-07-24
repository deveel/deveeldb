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
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CallStatement  : SqlStatement, IPlSqlStatement {
		public CallStatement(ObjectName procedureName) 
			: this(procedureName, new InvokeArgument[0]) {
		}

		public CallStatement(ObjectName procedureName, InvokeArgument[] arguments) {
			ProcedureName = procedureName;
			Arguments = arguments;
		}

		public CallStatement(ObjectName procedureName, SqlExpression[] arguments)
			: this(procedureName, arguments == null ? new InvokeArgument[0] : arguments.Select(x => new InvokeArgument(x)).ToArray()) {
		}

		private CallStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ProcedureName = (ObjectName) info.GetValue("Procedure", typeof (ObjectName));
			Arguments = (InvokeArgument[]) info.GetValue("Arguments", typeof (InvokeArgument[]));
		}

		public ObjectName ProcedureName { get; private set; }

		public InvokeArgument[] Arguments { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Procedure", ProcedureName);

			var args = new InvokeArgument[0];
			if (Arguments != null)
				args = Arguments.ToArray();

			info.AddValue("Arguments", args);
		}

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var args = Arguments;
			if (args != null) {
				var newArgs = new List<InvokeArgument>();
				foreach (var arg in args) {
					newArgs.Add((InvokeArgument)((IPreparable)arg).Prepare(preparer));
				}

				args = newArgs.ToArray();
			}

			return new CallStatement(ProcedureName, args);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var procedureName = context.Access().ResolveObjectName(DbObjectType.Routine, ProcedureName);

			return new CallStatement(procedureName, Arguments);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var args = Arguments != null ? Arguments.ToArray() : new InvokeArgument[0];
			var invoke = new Invoke(ProcedureName, args);

			var procedure = context.DirectAccess.ResolveProcedure(invoke);
			if (procedure == null)
				throw new StatementException(String.Format("Could not retrieve the procedure '{0}': maybe not a procedure.", ProcedureName));

			if (!context.User.CanExecuteProcedure(invoke,context.Request))
				throw new MissingPrivilegesException(context.User.Name, ProcedureName, Privileges.Execute);

			var result = procedure.Execute(Arguments, context.Request);

			if (result.HasOutputParameters) {
				var output = result.OutputParameters;
				var names = output.Keys.ToArray();
				var values = output.Values.Select(SqlExpression.Constant).Cast<SqlExpression>().ToArray();
				var resultTable = new FunctionTable(values, names, context.Request);
				context.SetResult(resultTable);
			}
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.AppendFormat("CALL {0}(", ProcedureName);
			if (Arguments != null &&
				Arguments.Length > 0) {
				for (int i = 0; i < Arguments.Length; i++) {
					Arguments[i].AppendTo(builder);

					if (i < Arguments.Length - 1)
						builder.Append(", ");
				}
			}

			builder.Append(")");
		}
	}
}
