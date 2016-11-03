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

namespace Deveel.Data.Security {
	public sealed class AssertResult {
		private AssertResult(AccessPolicy policy, Exception error) {
			Policy = policy;
			Error = error;
		}

		public AccessPolicy Policy { get; private set; }

		public Exception Error { get; private set; }

		public bool IsAllowed {
			get { return Policy == AccessPolicy.Allow; }
		}

		public bool IsDenied {
			get { return Policy == AccessPolicy.Deny; }
		}

		public static AssertResult Allow() {
			return new AssertResult(AccessPolicy.Allow, null);
		}

		public static AssertResult Deny(Exception error) {
			return new AssertResult(AccessPolicy.Deny, error);
		}

		public static AssertResult Deny(string errorMessage) {
			return Deny(new SecurityException(errorMessage));
		}

		public static AssertResult Deny() {
			return new AssertResult(AccessPolicy.Deny, null);
		}
	}
}
