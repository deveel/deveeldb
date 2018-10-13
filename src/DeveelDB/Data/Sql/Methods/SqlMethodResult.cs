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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Methods {
	public sealed class SqlMethodResult {
		internal SqlMethodResult(SqlExpression returned, bool hasReturn, IDictionary<string, SqlExpression> output) {
			ReturnedValue = returned;
			HasReturnedValue = hasReturn;
			var copyOutput = new Dictionary<string, SqlExpression>();
			foreach (var pair in output) {
				copyOutput[pair.Key] = pair.Value;
			}
			Output = new ReadOnlyDictionary<string, SqlExpression>(copyOutput);
		}

		public SqlExpression ReturnedValue { get; }

		public bool HasReturnedValue { get; }

		public IDictionary<string, SqlExpression> Output { get; }

		internal void Validate(SqlMethod method, QueryContext context) {
			var methodInfo = method.MethodInfo;

			if (method.IsFunction && !HasReturnedValue)
				throw new MethodException($"The execution of function {methodInfo.MethodName} has no returned value");

			var output = methodInfo.Parameters.Where(x => x.IsOutput);
			foreach (var requestedParam in output) {
				SqlExpression outputValue;
				if (!Output.TryGetValue(requestedParam.Name, out outputValue))
					throw new MethodException($"The requested output parameter {requestedParam.Name} was not set by the method {methodInfo.MethodName}");

				var outputType = outputValue.GetSqlType(context);
				if (!outputType.IsComparable(requestedParam.ParameterType))
					throw new MethodException($"The value set for output parameter {requestedParam.Name} is invalid");
			}
		}
	}
}