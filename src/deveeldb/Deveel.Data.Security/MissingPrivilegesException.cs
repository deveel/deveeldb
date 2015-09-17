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

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public class MissingPrivilegesException : SecurityException {
		public MissingPrivilegesException(string userName, ObjectName objectName)
			: this(userName, objectName, Privileges.None) {
		}

		public MissingPrivilegesException(string userName, ObjectName objectName, Privileges privileges)
			: this(userName, objectName, privileges, MakeMessage(userName, objectName, privileges)) {
		}

		public MissingPrivilegesException(string userName, ObjectName objectName, string message)
			: this(userName, objectName, Privileges.None, message) {
		}

		public MissingPrivilegesException(string userName, ObjectName objectName, Privileges privileges, string message)
			: base(SecurityErrorCodes.MissingPrivileges, message) {
			UserName = userName;
			ObjectName = objectName;
			Privileges = privileges;
		}

		public string UserName { get; private set; }

		public ObjectName ObjectName { get; private set; }

		public Privileges Privileges { get; private set; }

		private static string MakeMessage(string userName, ObjectName objectName, Privileges privileges) {
			if (privileges == Privileges.None)
				return String.Format("User '{0}' has not enough privileges to operate on the object '{1}'.", userName, objectName);

			return String.Format("User '{0}' has not the privilege '{1}' on the object '{2}'.", userName, privileges, objectName);
		}
	}
}
