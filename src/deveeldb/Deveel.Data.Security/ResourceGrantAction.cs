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

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class ResourceGrantAction : ISecurityPostExecuteAction {
		public ResourceGrantAction(ObjectName resourceName, DbObjectType resourceType, Privileges privileges) {
			ResourceName = resourceName;
			ResourceType = resourceType;
			Privileges = privileges;
		}

		public ObjectName ResourceName { get; private set; }

		public DbObjectType ResourceType { get; private set; }

		public Privileges Privileges { get; private set; }

		void ISecurityPostExecuteAction.OnActionExecuted(ISecurityContext context) {
			try {
				context.Request.Access().GrantOn(ResourceType, ResourceName, context.User.Name, Privileges, true);
			} catch (SecurityException) {
				throw;
			} catch (Exception ex) {
				throw new SecurityException(
					String.Format("An error occurred while granting '{0}' to '{1}' on '{2}'.", Privileges, ResourceName,
						context.User.Name), ex);
			}
		}
	}
}
