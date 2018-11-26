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

namespace Deveel.Data.Sql.Parsing {
	class SqlParseElementNode  {
		public ObjectName Id { get; set; }

		public SqlParseFunctionArgument[] Argument { get; set; }

		public static SqlParseElementNode Form(IContext context, PlSqlParser.General_elementContext element) {
			var id = SqlParseName.Object(element.objectName());
			var arg = element.function_argument();
			IEnumerable<SqlParseFunctionArgument> argNodes = null;
			if (arg != null) {
				argNodes = arg.argument().Select(x => SqlParseFunctionArgument.Form(context, x));
			}

			return new SqlParseElementNode {
				Id = id,
				Argument = argNodes != null ? argNodes.ToArray() : null
			};
		}
	}
}
