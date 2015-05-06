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

namespace Deveel.Data.Security {
	[Serializable]
	public class MissingPrivilegesException : SecurityException {
		public MissingPrivilegesException()
			: this((ObjectName)null) {
		}

		public MissingPrivilegesException(ObjectName objectName)
			: this(objectName, MakeMessage(objectName)) {
		}

		public MissingPrivilegesException(string message)
			: this(null, message) {
		}

		public MissingPrivilegesException(ObjectName objectName, string message)
			: base(SecurityErrorCodes.MissingPrivileges, message) {
			ObjectName = objectName;
		}

		public ObjectName ObjectName { get; private set; }

		private static string MakeMessage(ObjectName objectName) {
			if (objectName == null)
				return "User has not enough privileges to execute the operation.";

			return String.Format("User has not enough privileges to operate on '{0}'.", objectName);
		}
	}
}
