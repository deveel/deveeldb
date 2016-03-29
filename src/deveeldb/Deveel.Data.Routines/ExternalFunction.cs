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
using System.Reflection;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public sealed class ExternalFunction : Function {
		public ExternalFunction(ExternalFunctionInfo functionInfo) 
			: base(functionInfo) {
			functionInfo.ExternalRef.CheckReference(functionInfo);
		}

		public ExternalRef ExternalRef {
			get { return ((ExternalFunctionInfo) FunctionInfo).ExternalRef; }
		}

		public override InvokeResult Execute(InvokeContext context) {
			var args = context.EvaluatedArguments;

			var method = ExternalRef.GetMethod();

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
				if ((paramType == typeof (ISession) ||
					paramType == typeof(IRequest) ||
					paramType == typeof(IQuery)) && 
					i > 0)
						throw new InvalidOperationException("The request parameter must be the first in method signature.");

				object arg;

				if (paramType == typeof (ISession)) {
					arg = request.Query.Session;
				} else if (paramType == typeof (IQuery)) {
					arg = request.Query;
				} else if (paramType == typeof(IRequest)) { 
					arg = new Block(request);
				} else {
					var sqlType = PrimitiveTypes.FromType(paramType);
					arg = sqlType.ConvertTo(args[i].Value, paramType);
				}

				values[i] = arg;
			}

			return values;
		}

		public override SqlType ReturnType(InvokeContext context) {
			var returnType = FunctionInfo.ReturnType;
			if (returnType == null) {
				var methodReturnType = ExternalRef.GetMethod().ReturnType;
				returnType = PrimitiveTypes.FromType(methodReturnType);
			}

			return returnType;
		}
	}
}
