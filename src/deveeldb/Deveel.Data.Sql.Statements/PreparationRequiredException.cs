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

namespace Deveel.Data.Sql.Statements {
	public class PreparationRequiredException : StatementException {
		public string TypeName { get; set; }

		public PreparationRequiredException(string typeName)
			: this(typeName, String.Format("The Statement '{0}' requires preparation before being executed", typeName)) {
		}

		public PreparationRequiredException(string typeName, string message)
			: base(message) {
			TypeName = typeName;
		}
	}
}
