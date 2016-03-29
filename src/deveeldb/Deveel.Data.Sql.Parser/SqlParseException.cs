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

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// An error that occurs when compiling a input string into
	/// a SQL object.  
	/// </summary>
	class SqlParseException : Exception {
		public SqlParseException(string message, int line, int column)
			: this(message, SqlParseErrorLevel.Error, line, column) {
		}

		public SqlParseException(string message, SqlParseErrorLevel level, int line, int column)
			: base(message) {
			Level = level;
			Line = line;
			Column = column;
		}

		public SqlParseErrorLevel Level { get; private set; }

		public int Line { get; private set; }

		public int Column { get; private set; }
	}
}
