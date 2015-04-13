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

using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	public sealed class ExternalFunction : Function {
		private MethodInfo method;

		public ExternalFunction(FunctionInfo functionInfo) 
			: base(functionInfo) {
			if (functionInfo.FunctionType != FunctionType.External)
				throw new ArgumentException("The information specified are not pointing to any external function.");
		}

		protected override DataObject Evaluate(DataObject[] args) {
			if (method == null)
				method = DiscoverMethod();

			if (method == null)
				throw new InvalidOperationException();

			try {
				var methodArgs = ConvertArguments(method, args);
				var result = method.Invoke(null, methodArgs);

				return ConvertValue(result, ReturnType());
			} catch (Exception ex) {
				throw;
			}
		}

		private DataObject ConvertValue(object value, DataType sqlType) {
			throw new NotImplementedException();
		}

		private object[] ConvertArguments(MethodInfo methodInfo, DataObject[] args) {
			var methodParams = methodInfo.GetParameters();

			if (methodParams.Length != args.Length)
				throw new InvalidOperationException();		// TODO: support the case of the Unbounded parameters

			var values = new object[args.Length];
			for (int i = 0; i < args.Length; i++) {
				var paramType = methodParams[i].ParameterType;
				var sqlType = PrimitiveTypes.FromType(paramType);

				values[i] = sqlType.ConvertTo(args[i].Value, paramType);
			}

			return values;
		}

		public override DataType ReturnType(ExecuteContext context) {
			if (method == null)
				method = DiscoverMethod();

			return base.ReturnType(context);
		}

		private MethodInfo DiscoverMethod() {
			var type = FunctionInfo.ExternalType;
			var methodName = FunctionInfo.ExternalMethodName;
			if (String.IsNullOrEmpty(methodName))
				methodName = FunctionInfo.RoutineName.Name;

			throw new NotImplementedException();
		}
	}
}
