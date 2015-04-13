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

namespace Deveel.Data.Diagnostics {
	public static class CommandTypes {
		public const string CreateSchema = "create-schema";
		public const string DropSchema = "drop-schema";
		public const string CreateTable = "create-table";
		public const string AlterTable = "alter-table";
		public const string DropTable = "drop-table";
		public const string CreateSequence = "create-sequence";
		public const string DropSequence = "drop-sequence";
	}
}
