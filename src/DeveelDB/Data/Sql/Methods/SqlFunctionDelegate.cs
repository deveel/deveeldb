// 
//  Copyright 2010-2017 Deveel
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

namespace Deveel.Data.Sql.Methods {
	public sealed class SqlFunctionDelegate : SqlFunctionBase {
		private readonly Func<MethodContext, Task> body;

		public SqlFunctionDelegate(SqlFunctionInfo functionInfo, Func<MethodContext, Task> body)
			: base(functionInfo) {
			this.body = body;
		}

		public SqlFunctionDelegate(SqlFunctionInfo functionInfo, Func<MethodContext, Task<SqlObject>> body)
			: this(functionInfo, async context => {
				var result = await body(context);
				context.SetResult(result);
			}) {
		}

		public SqlFunctionDelegate(SqlFunctionInfo functionInfo, Func<MethodContext, SqlExpression> body)
			: this(functionInfo, context => {
				var result = body(context);
				context.SetResult(result);
				return Task.CompletedTask;
			}) {
		}

		public override FunctionType FunctionType => FunctionType.Scalar;

		protected override Task ExecuteContextAsync(MethodContext context) {
			return body(context);
		}
	}
}