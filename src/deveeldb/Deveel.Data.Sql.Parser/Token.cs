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

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// This is a single token within a string parsed.
	/// </summary>
	/// <remarks>
	/// Tokens are the very basic elements of a string analysis and
	/// handle the location within the string context, and the value 
	/// they represent, which is a single word, character or number.
	/// </remarks>
	public sealed class Token {
		internal Token(int column, int line, string text, object value) {
			Text = text;
			Value = value;
			Line = line;
			Column = column;
		}

		/// <summary>
		/// Gets the column location within the <see cref="Line"/> 
		/// of the token in the string context it belongs to.
		/// </summary>
		public int Column { get; private set; }

		/// <summary>
		/// Gets the line number of the token.
		/// </summary>
		public int Line { get; private set; }

		/// <summary>
		/// Gets the text that represents the token.
		/// </summary>
		public string Text { get; private set; }

		/// <summary>
		/// Gets the value of the token.
		/// </summary>
		public object Value { get; private set; }

		/// <inheritdoc/>
		public override string ToString() {
			return Text;
		}
	}
}