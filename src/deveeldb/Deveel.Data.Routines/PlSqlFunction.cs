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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public sealed class PlSqlFunction : Function {
		public PlSqlFunction(PlSqlFunctionInfo functionInfo) 
			: base(functionInfo) {
		}

		public SqlStatement Body {
			get { return ((PlSqlFunctionInfo) FunctionInfo).Body; }
		}

		public override InvokeResult Execute(InvokeContext context) {
			var execContext = new ExecutionContext(context.Request, Body);
			Body.Execute(execContext);

			if (!execContext.HasResult)
				throw new InvalidOperationException("The execution of the function has no returns");

			var result = execContext.Result;
			var returnType = ReturnType(context);

			if (returnType is TabularType)
				return context.Result(result);

			if (result.RowCount == 0)
				throw new InvalidOperationException("The execution of the function has no returns");

			var retunValue = result.GetValue(0, 0);
			return context.Result(retunValue);
		}
	}
}
