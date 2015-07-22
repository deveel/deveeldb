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

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// An error that occurs when compiling a input string into
	/// a SQL object.  
	/// </summary>
	[Serializable]
	public sealed class SqlParseException : ErrorException {
		public SqlParseException() 
			: this(CompileErrorCodes.SyntaxError) {
		}

		public SqlParseException(int errorCode) 
			: base(EventClasses.Compiler, errorCode) {
		}

		public SqlParseException(string message) 
			: this(CompileErrorCodes.SyntaxError, message) {
		}

		public SqlParseException(int errorCode, string message) 
			: base(EventClasses.Compiler, errorCode, message) {
		}

		public SqlParseException(string message, Exception innerException) 
			: this(CompileErrorCodes.SyntaxError, message, innerException) {
		}

		public SqlParseException(int errorCode, string message, Exception innerException) 
			: base(EventClasses.Compiler, errorCode, message, innerException) {
		}
	}
}
