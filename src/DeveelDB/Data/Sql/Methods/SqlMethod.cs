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
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Methods {
	public abstract class SqlMethod : ISqlFormattable {
		protected SqlMethod(SqlMethodInfo methodInfo) {
			if (methodInfo == null)
				throw new ArgumentNullException(nameof(methodInfo));

			MethodInfo = methodInfo;
		}

		public SqlMethodInfo MethodInfo { get; }

		public abstract MethodType Type { get; }

		public bool IsFunction => Type == MethodType.Function;

		public bool IsProcedure => Type == MethodType.Procedure;

		public virtual bool IsSystem => true;

		protected virtual bool ValidateInvoke(InvokeInfo invokeInfo) {
			var required = MethodInfo.Parameters.Where(x => x.IsRequired).ToDictionary(x => x.Name, y => y);
			foreach (var param in required) {
				if (!invokeInfo.HasArgument(param.Key))
					return false;

				if (!param.Value.IsDeterministic) {
					var argType = invokeInfo.ArgumentType(param.Key);
					if (!param.Value.ParameterType.IsComparable(argType))
						return false;
				}
			}

			return true;
		}

		public async Task<SqlMethodResult> ExecuteAsync(QueryContext context, Invoke invoke) {
			using (var methodContext = new MethodContext(context, this, invoke)) {
				try {
					await ExecuteContextAsync(methodContext);
				} catch (MethodException) {
					throw;
				} catch (Exception ex) {
					throw new MethodException($"Error while executing {MethodInfo.MethodName}: see inner exception for more information", ex);
				}

				var result = methodContext.CreateResult();

				result.Validate(this, context);

				return result;
			}
		}

		public async Task<SqlMethodResult> ExecuteAsync(QueryContext context, params InvokeArgument[] args) {
			var invoke = new Invoke(MethodInfo.MethodName);
			foreach (var arg in args) {
				invoke.Arguments.Add(arg);
			}

			return await ExecuteAsync(context, invoke);
		}

		public Task<SqlMethodResult> ExecuteAsync(QueryContext context, params SqlExpression[] args) {
			var invokeArgs = args == null ? new InvokeArgument[0] : args.Select(x => new InvokeArgument(x)).ToArray();
			return ExecuteAsync(context, invokeArgs);
		}

		public Task<SqlMethodResult> ExecuteAsync(QueryContext context, params SqlObject[] args) {
			var exps = args == null
				? new SqlExpression[0]
				: args.Select(SqlExpression.Constant).Cast<SqlExpression>().ToArray();
			return ExecuteAsync(context, exps);
		}

		protected abstract Task ExecuteContextAsync(MethodContext context);

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		protected virtual void AppendTo(SqlStringBuilder builder) {
			builder.Append(Type.ToString().ToUpperInvariant());
			builder.Append(" ");

			MethodInfo.AppendTo(builder);
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		public virtual bool Matches(QueryContext context, Invoke invoke) {
			return MethodInfo.Matches(context, ValidateInvoke, invoke);
		}
	}
}