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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Security {
	public sealed class SecurityAssertionResult {
		private readonly IEnumerable<AssertResult> results;

		internal SecurityAssertionResult(IEnumerable<AssertResult> results) {
			this.results = new List<AssertResult>(results);
		}

		public bool IsAllowed {
			get { return !results.Any() || results.All(x => x.IsAllowed); }
		}

		public bool IsDenied {
			get { return results.Any(x => x.IsDenied); }
		}

		public Exception[] Errors {
			get { return results.Where(x => x.IsDenied && x.Error != null).Select(x => x.Error).ToArray(); }
		}

		public SecurityException SecurityError {
			get {
				var error = Errors.OfType<SecurityException>().FirstOrDefault();
				if (error == null) {
					var innerError = Errors.FirstOrDefault();
					error = new SecurityException("An error occurred while performing a security assertion", innerError);
				}

				return error;
			}
		}
	}
}
