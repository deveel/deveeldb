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

using Antlr4.Runtime.Misc;
using Antlr4.Runtime.Tree;

namespace Deveel.Data.Sql.Compile {
	static class Number {
		public static int? PositiveInteger(PlSqlParser.NumericContext context) {
			if (context == null)
				return null;

			var text = context.GetText();
			int value;
			if (!Int32.TryParse(text, out value))
				throw new ParseCanceledException(String.Format("Numeric '{0}' is not an integer.", text));
			if (value < 0)
				throw new ParseCanceledException(String.Format("Integer '{0}' is not positive.", text));

			return value;
		}

		public static int? PositiveInteger(ITerminalNode node) {
			if (node == null)
				return null;

			var text = node.GetText();
			int value;
			if (!Int32.TryParse(text, out value))
				throw new ParseCanceledException(String.Format("Numeric '{0}' is not an integer.", text));
			if (value < 0)
				throw new ParseCanceledException(String.Format("Integer '{0}' is not positive.", text));

			return value;
		}

		public static int? Integer(PlSqlParser.NumericContext context) {
			if (context == null)
				return null;

			var text = context.GetText();
			int value;
			if (!Int32.TryParse(text, out value))
				throw new ParseCanceledException(String.Format("Numeric '{0}' is not an integer.", text));

			return value;
		}
	}
}
