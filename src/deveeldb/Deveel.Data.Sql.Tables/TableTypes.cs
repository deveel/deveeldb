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

namespace Deveel.Data {
	/// <summary>
	/// Provides the constant names of the types of tables
	/// in a database system.
	/// </summary>
	public static class TableTypes {
		public const string Table = "TABLE";
		public const string SystemTable = "SYSTEM TABLE";
		public const string View = "VIEW";
		public const string Trigger = "TRIGGER";
		public const string Sequence = "SEQUENCE";
		public const string Procedure = "PROCEDURE";
		public const string Function = "FUNCTION";
	}
}
