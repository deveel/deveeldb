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

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parsing {
	public sealed class SqlParseMessage {
		public SqlParseMessage(string message, SqlParseMessageLevel level) 
			: this(null, message, level) {
		}

		public SqlParseMessage(string code, string message, SqlParseMessageLevel level) 
			: this(code, message, level, null) {
		}

		public SqlParseMessage(string message, SqlParseMessageLevel level, LocationInfo location) 
			: this(null, message, level, location) {
		}

		public SqlParseMessage(string code, string message, SqlParseMessageLevel level, LocationInfo location) {
			Code = code;
			Message = message;
			Level = level;
			Location = location;
		}

		public LocationInfo Location { get; }

		public string Message { get; }

		public string Code { get; }

		public SqlParseMessageLevel Level { get; }

		public bool HasLocation => Location != null;

		public bool HasCode => !String.IsNullOrWhiteSpace(Code);
	}
}