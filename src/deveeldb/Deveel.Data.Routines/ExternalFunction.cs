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
		private readonly MethodInfo method;

		public ExternalFunction(FunctionInfo functionInfo) 
			: base(functionInfo) {
			if (functionInfo.FunctionType != FunctionType.External)
				throw new ArgumentException("The information specified are not pointing to any external function.");

			method = DiscoverMethod();

			if (method == null)
				throw new InvalidOperationException(String.Format("Cannot resolve method '{0}.{1}' for external function '{2}'.",
					functionInfo.ExternalType, functionInfo.ExternalMethodName, functionInfo.RoutineName));

			if (method.ReturnType == typeof (void))
				throw new InvalidOperationException(String.Format("The method '{0}.{1}' is not a function.",
					functionInfo.ExternalType, functionInfo.ExternalMethodName));
		}

		public override InvokeResult Execute(InvokeContext context) {
			var args = context.EvaluatedArguments;

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
			var returnType = method.ReturnType;

			return PrimitiveTypes.FromType(returnType);
		}

		private MethodInfo DiscoverMethod() {
			var type = FunctionInfo.ExternalType;
			var methodName = FunctionInfo.ExternalMethodName;
			if (String.IsNullOrEmpty(methodName))
				methodName = FunctionInfo.RoutineName.Name;

			MethodInfo foundMethod = null;

			var methods = type.GetMethods(BindingFlags.Public | BindingFlags.Static);
			foreach (var methodInfo in methods) {
				if (methodInfo.Name == methodName) {
					var methodPars = methodInfo.GetParameters();
					if (ParametersMatch(methodPars)) {
						if (foundMethod != null)
							throw new AmbiguousMatchException(String.Format("Ambiguous reference to method '{0}' in type '{1}'.", methodName, type));

						foundMethod = methodInfo;
					}
				}
			}

			return foundMethod;
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
