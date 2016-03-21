// 
//  Copyright 2010-2015 Deveel
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
using System.Reflection;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public sealed class ExternalFunction : Function {
		public ExternalFunction(FunctionInfo functionInfo) 
			: base(functionInfo) {
			if (functionInfo.FunctionType != FunctionType.External)
				throw new ArgumentException("The information specified are not pointing to any external function.");

			CheckReference();
		}

		private void CheckReference() {
			var method = FunctionInfo.ExternalRef.GetMethod();
			if (method == null)
				throw new ArgumentException(String.Format("The reference '{0}' does not resolve to any method.", FunctionInfo.ExternalRef));

			if (method.ReturnType == typeof (void))
				throw new ArgumentException(String.Format("The method '{0}.{1}' is not a function.",
					FunctionInfo.ExternalRef.Type.FullName, method.Name));

			var methodParams = method.GetParameters();
			if (!ParametersMatch(methodParams))
				throw new ArgumentException("The parameters of this function and the reference do not match.");
		}

		public override InvokeResult Execute(InvokeContext context) {
			var args = context.EvaluatedArguments;

			var method = FunctionInfo.ExternalRef.GetMethod();

			var methodArgs = ConvertArguments(method, context.Request, args);
			var result = method.Invoke(null, methodArgs);

			return context.Result(ConvertValue(result, ReturnType()));
		}

		private Field ConvertValue(object value, SqlType sqlType) {
			var obj = sqlType.CreateFrom(value);
			return new Field(sqlType, obj);
		}

		private object[] ConvertArguments(MethodInfo methodInfo, IRequest request, Field[] args) {
			var methodParams = methodInfo.GetParameters();

			if (methodParams.Length != args.Length)
				throw new InvalidOperationException();		// TODO: support the case of the Unbounded parameters

			var values = new object[args.Length];
			for (int i = 0; i < args.Length; i++) {
				var paramType = methodParams[i].ParameterType;
				if (paramType == typeof (ISession)) {
					if (i > 0)
						throw new InvalidOperationException("The request parameter must be the first in method signature.");

					values[i] = request;
				} else {
					var sqlType = PrimitiveTypes.FromType(paramType);
					values[i] = sqlType.ConvertTo(args[i].Value, paramType);
				}
			}

			return values;
		}

		public override SqlType ReturnType(InvokeContext context) {
			var returnType = FunctionInfo.ReturnType;
			if (returnType == null) {
				var methodReturnType = FunctionInfo.ExternalRef.GetMethod().ReturnType;
				returnType = PrimitiveTypes.FromType(methodReturnType);
			}

			return returnType;
		}

		private bool ParametersMatch(ParameterInfo[] parameters) {
			var routineParameters = FunctionInfo.Parameters;
			if ((routineParameters == null || routineParameters.Length == 0) &&
			    (parameters == null || parameters.Length == 0))
				return true;

			if (routineParameters == null || routineParameters.Length == 0)
				return false;

			var offset = 0;
			if (parameters[0].ParameterType == typeof (ISession))
				offset = 1;

			if (routineParameters.Length != parameters.Length - offset)
				return false;

			for (int i = offset; i < parameters.Length; i++) {
				var param = parameters[i];

				var paramType = PrimitiveTypes.FromType(param.ParameterType);
				var routineParameter = routineParameters[i];

				if (!routineParameter.Type.CanCastTo(paramType))
					return false;
			}

			return true;
		}
	}
}
