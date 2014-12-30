// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	[Serializable]
	public sealed class UserGrant {
		public UserGrant(string userName, Privileges privileges, ObjectName objectName, DbObjectType objectType, string granterName) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(granterName))
				throw new ArgumentNullException("granterName");
			if (objectName == null)
				throw new ArgumentNullException("objectName");

			UserName = userName;
			Privileges = privileges;
			ObjectName = objectName;
			ObjectType = objectType;
			GranterName = granterName;
		}

		public string UserName { get; private set; }

		public string GranterName { get; private set; }

		public ObjectName ObjectName { get; private set; }

		public DbObjectType ObjectType { get; private set; }

		public Privileges Privileges { get; private set; }
	}
}