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
