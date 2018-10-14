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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Sql.Methods {
	public sealed class MethodContext : Context, IVariableResolver {
		private Dictionary<string, SqlExpression> output;
		private Dictionary<string, SqlExpression> namedArgs;

		internal MethodContext(QueryContext context, SqlMethod method, Invoke invoke)
			: base(context) {
			Invoke = invoke;
			Method = method;

			namedArgs = BuildArguments(method.MethodInfo, invoke);

			ResultValue = SqlExpression.Constant(SqlObject.Null);
			output = new Dictionary<string, SqlExpression>();

			Metadata = new Dictionary<string, object>();
		}

		public SqlMethod Method { get; }

		protected override string Name => $"Method({Method.MethodInfo.MethodName})";

		public Invoke Invoke { get; private set; }

		internal SqlExpression ResultValue { get; private set; }

		internal bool HasResult { get; private set; }

		public int ArgumentCount => Invoke.Arguments.Count;

		public IDictionary<string, object> Metadata { get; }

		private QueryContext QueryContext => Parent as QueryContext;

		public SqlExpression Argument(string argName) {
			SqlExpression value;
			if (!namedArgs.TryGetValue(argName, out value)) {
				throw new InvalidOperationException();
			}

			return value;
		}

		public SqlExpression Argument(int offset) {
			if (offset >= Invoke.Arguments.Count)
				throw new ArgumentOutOfRangeException();

			return Invoke.Arguments[offset].Value;
		}

		public SqlObject Value(string argName) {
			var exp = Argument(argName);
			var value = exp.Reduce(QueryContext);

			if (!(value is SqlConstantExpression))
				throw new InvalidOperationException($"The argument {argName} of the invoke does not resolve to any constant value");

			return ((SqlConstantExpression) value).Value;
		}

		public SqlObject Value(int offset) {
			var exp = Argument(offset);
			var value = exp.Reduce(QueryContext);

			if (!(value is SqlConstantExpression))
				throw new InvalidOperationException($"The argument at offset {offset} of the invoke does not resolve to any constant value");

			return ((SqlConstantExpression)value).Value;
		}

		private static Dictionary<string, SqlExpression> BuildArguments(SqlMethodInfo methodInfo, Invoke invoke) {
			var result = new Dictionary<string, SqlExpression>();

			if (invoke.IsNamed) {
				var invokeArgs = invoke.Arguments.ToDictionary(x => x.Name, y => y.Value);
				var methodParams = methodInfo.Parameters.ToDictionary(x => x.Name, y => y);

				foreach (var invokeArg in invokeArgs) {
					SqlParameterInfo paramInfo;
					if (!methodParams.TryGetValue(invokeArg.Key, out paramInfo))
						throw new InvalidOperationException(
							$"Invoke argument {invokeArg.Key} does not correspond to any parameter of the method");

					result[invokeArg.Key] = invokeArg.Value;
				}

				foreach (var methodParam in methodParams) {
					if (!result.ContainsKey(methodParam.Key)) {
						var paramInfo = methodParam.Value;
						if (paramInfo.IsRequired)
							throw new InvalidOperationException(
								$"The invoke to {methodInfo.MethodName} has no value for required parameter {paramInfo.Name} and no default value was set");

						result[methodParam.Key] = paramInfo.DefaultValue;
					}
				}
			} else {
				if (methodInfo.Parameters.Count != invoke.Arguments.Count)
					throw new NotSupportedException($"Invoke arguments mismatch the number of parameters of {methodInfo.MethodName}");

				for (int i = 0; i < methodInfo.Parameters.Count; i++) {
					var parmInfo = methodInfo.Parameters[i];
					result[parmInfo.Name] = invoke.Arguments[i].Value;
				}
			}

			return result;
		}

		Variable IVariableResolver.ResolveVariable(string name, bool ignoreCase) {
			SqlParameterInfo paramInfo;
			if (!Method.MethodInfo.TryGetParameter(name, ignoreCase, out paramInfo))
				return null;

			SqlExpression value;
			if (!namedArgs.TryGetValue(name, out value)) {
				value = SqlExpression.Constant(SqlObject.Null);
			}

			return new Variable(name, paramInfo.ParameterType, true, value);
		}

		SqlType IVariableResolver.ResolveVariableType(string name, bool ignoreCase) {
			SqlParameterInfo paramInfo;
			if (!Method.MethodInfo.TryGetParameter(name, ignoreCase, out paramInfo))
				return null;

			return paramInfo.ParameterType;
		}

		internal SqlMethodResult CreateResult() {
			return new SqlMethodResult(ResultValue, HasResult, output);
		}

		public void SetOutput(string parameterName, SqlExpression value) {
			if (String.IsNullOrWhiteSpace(parameterName))
				throw new ArgumentNullException(nameof(parameterName));

			if (!Method.IsProcedure)
				throw new InvalidOperationException($"The method {Method.MethodInfo.MethodName} is not a Procedure");

			SqlParameterInfo parameter;
			if (!Method.MethodInfo.Parameters.ToDictionary(x => x.Name, y => y).TryGetValue(parameterName, out parameter))
				throw new ArgumentException($"The method {Method.MethodInfo.MethodName} contains no parameter {parameterName}");

			if (!parameter.IsOutput)
				throw new ArgumentException($"The parameter {parameter.Name} is not an OUTPUT parameter");

			output[parameterName] = value;
		}

		public void SetResult(SqlObject value) {
			if (!Method.IsFunction)
				throw new InvalidOperationException();

			if (value.IsNull) {
				var functionInfo = (SqlFunctionInfo) Method.MethodInfo;
				value = SqlObject.NullOf(functionInfo.ReturnType);
			}

			SetResult(SqlExpression.Constant(value));
		}

		public void SetResult(SqlExpression value) {
			if (!Method.IsFunction)
				throw new InvalidOperationException($"Trying to set the return type to the method {Method.MethodInfo.MethodName} that is not a function.");

			var functionInfo = (SqlFunctionInfo)Method.MethodInfo;

			if (value.ExpressionType == SqlExpressionType.Constant) {
				var exp = (SqlConstantExpression) value;
				if (exp.Value.IsNull)
					value = SqlExpression.Constant(SqlObject.NullOf(functionInfo.ReturnType));
				if (exp.Value.IsUnknown || exp.Value.IsNull) {
					ResultValue = value;
					HasResult = true;
					return;
				}
			}

			var resultType = ((SqlFunctionBase) Method).ReturnType(QueryContext, Invoke);

			var valueType = value.GetSqlType(QueryContext);
			if (!resultType.IsComparable(valueType))
				throw new InvalidOperationException($"The result type {valueType} of the expression is not compatible " +
				                                    $"with the return type {functionInfo.ReturnType} of the function {Method.MethodInfo.MethodName}");


			// TODO: eventually CAST to the return type

			ResultValue = value;
			HasResult = true;
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (output != null)
					output.Clear();
			}

			output = null;
			ResultValue = null;
			base.Dispose(disposing);
		}
	}
}