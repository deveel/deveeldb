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

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Methods {
	public sealed class InvokeInfo {
		private readonly Dictionary<string, SqlType> arguments;

		internal InvokeInfo(SqlMethodInfo methodInfo, Dictionary<string, SqlType> arguments) {
			MethodInfo = methodInfo;
			this.arguments = arguments;
		}

		public SqlMethodInfo MethodInfo { get; }

		public IEnumerable<string> ArgumentNames => arguments.Keys;

		public bool HasArgument(string parameterName) {
			return arguments.ContainsKey(parameterName);
		}

		public SqlType ArgumentType(string parameterName) {
			SqlType type;
			if (!arguments.TryGetValue(parameterName, out type))
				return null;

			return type;
		}
	}
}