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
using System.Linq;

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// Extension methods to <see cref="ISqlNode"/> for diagnostics and other purposes.
	/// </summary>
	public static class SqlNodeExtensions {
		public static int StartColumn(this ISqlNode node) {
			if (node == null || node.Tokens == null)
				return 0;

			var token = node.Tokens.FirstOrDefault();
			return token == null ? 0 : token.Column;
		}

		public static int EndColumn(this ISqlNode node) {
			if (node == null || node.Tokens == null)
				return 0;

			var token = node.Tokens.LastOrDefault();
			return token == null ? 0 : token.Column;			
		}

		public static int StartLine(this ISqlNode node) {
			if (node == null || node.Tokens == null)
				return 0;

			var token = node.Tokens.FirstOrDefault();
			return token == null ? 0 : token.Line;
		}

		public static int EndLine(this ISqlNode node) {
			if (node == null || node.Tokens == null)
				return 0;

			var token = node.Tokens.LastOrDefault();
			return token == null ? 0 : token.Line;
		}
	}
}