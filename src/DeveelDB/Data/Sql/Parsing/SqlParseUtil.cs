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
using System.Text;

using Antlr4.Runtime.Misc;

namespace Deveel.Data.Sql.Parsing {
	static partial class SqlParseUtil {
		public static int? PositiveInteger(PlSqlParser.NumericContext context) {
			if (context == null)
				return null;

			var text = context.GetText();

			if (!Int32.TryParse(text, out var value))
				throw new ParseCanceledException($"Numeric '{text}' is not an integer.");
			if (value < 0)
				throw new ParseCanceledException($"Integer '{text}' is not positive.");

			return value;
		}
	}
}
