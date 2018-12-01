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
using System.Threading.Tasks;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements.Security {
	public sealed class PrivilegesRequirement : IRequirement {
		public PrivilegesRequirement(DbObjectType objectType, ObjectName objectName, Privilege privilege) {
			ObjectType = objectType;
			ObjectName = objectName ?? throw new ArgumentNullException(nameof(objectName));
			Privilege = privilege;
		}

		public Privilege Privilege { get; }

		public DbObjectType ObjectType { get; }

		public ObjectName ObjectName { get; }

		async Task IRequirement.HandleRequirementAsync(IContext context) {
			if (!await context.UserHasPrivileges(ObjectType, ObjectName, Privilege))
				throw new UnauthorizedAccessException($"The user '{context.User().Name}' misses the privilege {Privilege} on {ObjectName}");
		}
	}
}