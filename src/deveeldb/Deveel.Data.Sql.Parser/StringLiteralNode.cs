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

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// A node containing a constant literal string passed within
	/// an SQL command.
	/// </summary>
	[Serializable]
	public sealed class StringLiteralNode : SqlNode {
		internal StringLiteralNode() {
		}

		/// <summary>
		/// Gets the literal string value.
		/// </summary>
		public string Value { get; private set; }

		/// <inheritdoc/>
		protected override void OnNodeInit() {
			var text = Tokens.First().Text;
			if (!String.IsNullOrEmpty(text)) {
				if (text[0] == '\'' &&
				    text[text.Length - 1] == '\'')
					text = text.Substring(1, text.Length - 2);
			}

			Value = text;
			base.OnNodeInit();
		}
	}
}