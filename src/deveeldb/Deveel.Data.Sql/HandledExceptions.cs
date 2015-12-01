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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql {
	public sealed class HandledExceptions {
		public static readonly HandledExceptions Others = new HandledExceptions(null, true);

		private HandledExceptions(IEnumerable<string> exceptionNames, bool others) {
			if (!others) {
				if (exceptionNames == null)
					throw new ArgumentNullException("exceptionNames");

				if (exceptionNames.Any(String.IsNullOrEmpty))
					throw new ArgumentException();
			}

			ExceptionNames = exceptionNames;
			IsForOthers = others;
		}

		public HandledExceptions(IEnumerable<string> exceptionNames)
			: this(exceptionNames, false) {
		}

		public HandledExceptions(string exceptionName)
			: this(new[] {exceptionName}) {
		}

		public bool IsForOthers { get; private set; }

		public IEnumerable<string> ExceptionNames { get; private set; }
	}
}
