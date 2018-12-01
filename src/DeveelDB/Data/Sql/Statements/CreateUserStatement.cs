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

using Deveel.Data.Events;
using Deveel.Data.Security;
using Deveel.Data.Security.Events;
using Deveel.Data.Sql.Statements.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateUserStatement : SqlStatement {
		public CreateUserStatement(string userName, IUserIdentificationInfo identificationInfo) {
			UserName = userName ?? throw new ArgumentNullException(nameof(userName));
			IdentificationInfo = identificationInfo ?? throw new ArgumentNullException(nameof(identificationInfo));
		}

		public string UserName { get; }

		public IUserIdentificationInfo IdentificationInfo { get; }

		protected override async Task ExecuteStatementAsync(StatementContext context) {
			if (!User.IsValidName(UserName))
				throw new SqlStatementException($"The specified name '{UserName}' is invalid for a user");

			var securityManager = context.GetService<ISecurityManager>();
			if (securityManager == null)
				throw new SystemException("There is no security manager defined in the system");

			if (await securityManager.UserExistsAsync(UserName))
				throw new SqlStatementException($"A user named '{UserName}' already exists.");

			if (!await securityManager.CreateUserAsync(UserName, IdentificationInfo))
				throw new SqlStatementException($"It was not possible to create the user '{UserName}' because of a system error.");

			context.RaiseEvent(new UserCreatedEvent(this, UserName));
		}

		protected override void Require(IRequirementCollection requirements) {
			requirements.Require(x => x.UserIsAdmin());
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("CREATE USER ");
			builder.Append(UserName);
			builder.Append(" IDENTIFIED ");

			if (IdentificationInfo is PasswordIdentificationInfo) {
				builder.Append("BY '<password>'");
			} else {
				throw new NotSupportedException();
			}
		}
	}
}