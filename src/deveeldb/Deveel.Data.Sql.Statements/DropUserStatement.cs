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
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropUserStatement : SqlStatement {
		public DropUserStatement(string userName) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.Equals(userName, User.PublicName, StringComparison.OrdinalIgnoreCase) ||
				String.Equals(userName, User.SystemName, StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(String.Format("User '{0}' is reserved and cannot be dropped.", userName));

			UserName = userName;
		}

		private DropUserStatement(SerializationInfo info, StreamingContext context) {
			UserName = info.GetString("UserName");
		}

		public string UserName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.Request.Query.Session.SystemAccess.UserCanDropUser(UserName))
				throw new SecurityException(String.Format("The user '{0}' has not enough rights to drop the other user '{1}'",
					context.Request.Query.UserName(), UserName));

			if (!context.Request.Query.Session.SystemAccess.UserExists(UserName))
				throw new InvalidOperationException(String.Format("The user '{0}' does not exist: cannot delete.", UserName));

			context.Request.Query.Session.SystemAccess.DeleteUser(UserName);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("UserName", UserName);
		}
	}
}