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
using System.IO;
using System.Text;

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// Provides extensions to the <see cref="ISqlParser"/> 
	/// interface instances.
	/// </summary>
	public static class SqlParserExtensions {
		public static SqlParseResult Parse(this ISqlParser parser, Stream inputStream) {
			return Parse(parser, inputStream, Encoding.Unicode);
		}

		public static SqlParseResult Parse(this ISqlParser parser, Stream inputStream, Encoding encoding) {
			if (inputStream == null)
				throw new ArgumentNullException("inputStream");
			if (!inputStream.CanRead)
				throw new ArgumentException("The input stream cannot be read.", "inputStream");

			using (var reader = new StreamReader(inputStream, encoding)) {
				var text = reader.ReadToEnd();
				return parser.Parse(text);
			}
		}
	}
}
