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

using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Methods {
	public abstract class SqlFunctionBase : SqlMethod {
		protected SqlFunctionBase(SqlFunctionInfo functionInfo)
			: base(functionInfo) {
		}

		public new SqlFunctionInfo MethodInfo => (SqlFunctionInfo) base.MethodInfo;

		public abstract FunctionType FunctionType { get; }

		public bool IsAggregate => FunctionType == FunctionType.Aggregate;

		public override MethodType Type => MethodType.Function;

		public SqlType ReturnType(QueryContext context, Invoke invoke) {
			var returnType = MethodInfo.ReturnType;
			if (MethodInfo.IsDeterministic) {
				var invokeInfo = MethodInfo.GetInvokeInfo(context, invoke);
				returnType = DetermineReturnType(invokeInfo);
			}

			return returnType;
		}

		protected virtual SqlType DetermineReturnType(InvokeInfo invokeInfo) {
			SqlType resultType = null;
			foreach (var name in invokeInfo.ArgumentNames) {
				var argType = invokeInfo.ArgumentType(name);
				if (resultType == null) {
					resultType = argType;
				} else {
					resultType = argType.Wider(resultType);
				}
			}

			if (resultType == null)
				throw new MethodException($"Unable to determine the return type of function {MethodInfo.MethodName}");

			return resultType;
		}
	}
}