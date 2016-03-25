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

namespace Deveel.Data.Sql.Parser {
	class SqlParseError {
		public SqlParseError(string message, int line, int column) 
			: this(message, SqlParseErrorLevel.Error, line, column) {
		}

		public SqlParseError(string message, SqlParseErrorLevel level, int line, int column) {
			Message = message;
			Line = line;
			Column = column;
			Level = level;
		}

		public string Message { get; private set; }

		public int Line { get; private set; }

		public int Column { get; private set; }

		public Exception UnhandledException { get; set; }

		public SqlParseErrorLevel Level { get; private set; }

		public static SqlParseError Unhandled(Exception error) {
			return new SqlParseError("Unhandled error occurred", SqlParseErrorLevel.Critical, -1, -1) {
				UnhandledException = error
			};
		}
	}
}
