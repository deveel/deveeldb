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

using Deveel.Data.Query;
using Deveel.Data.Sql.Methods;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlFunctionExpression : SqlExpression {
		public ObjectName FunctionName { get; }

		public InvokeArgument[] Arguments { get; }

		internal SqlFunctionExpression(ObjectName functionName, InvokeArgument[] arguments)
			: base(SqlExpressionType.Function) {
			if (ObjectName.IsNullOrEmpty(functionName))
				throw new ArgumentNullException(nameof(functionName));

			if (arguments == null)
				arguments = new InvokeArgument[0];

			FunctionName = functionName;
			Arguments = arguments;
		}

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitFunction(this);
		}

		public override bool IsReference => true;

		public override bool CanReduce => true;

		protected override void AppendTo(SqlStringBuilder builder) {
			FunctionName.AppendTo(builder);
			builder.Append("(");

			for (int i = 0; i < Arguments.Length; i++) {
				Arguments[i].AppendTo(builder);

				if (i < Arguments.Length - 1)
					builder.Append(", ");
			}

			builder.Append(")");
		}

		private SqlFunctionBase ResolveFunction(QueryContext context) {
			if (context == null)
				throw new SqlExpressionException("A context is required to reduce a function invoke");

			var resolver = context.GetService<IMethodResolver>();
			if (resolver == null)
				throw new SqlExpressionException("No method resolver was configured in the scope");

			var name = FunctionName;
			if (!context.IsSystemFunction(name, Arguments)) {
				name = context.QualifyName(name);
			}

			var invoke = new Invoke(name, Arguments);
			var method = resolver.ResolveMethod(context, invoke);
			if (method == null)
				throw new SqlExpressionException($"Could not find any function for '{invoke}'.");

			if (!method.IsFunction)
				throw new SqlExpressionException($"The method {method.MethodInfo.MethodName} is not a function.");

			if (!method.Matches(context, invoke))
				throw new MethodException($"The function {method} was invoked with invalid arguments");

			return ((SqlFunctionBase) method);
		}

		public override SqlType GetSqlType(QueryContext context) {
			var function = ResolveFunction(context);
			return function.ReturnType(context, new Invoke(function.MethodInfo.MethodName, Arguments));
		}

		public override async Task<SqlExpression> ReduceAsync(QueryContext context) {
			var function = ResolveFunction(context);

			var result = await function.ExecuteAsync(context, Arguments);
			if (!result.HasReturnedValue)
				throw new SqlExpressionException();

			return result.ReturnedValue;
		}
	}
}