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
using Deveel.Data.Sql.Statements.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class AlterUserStatement : SqlStatement {
		public AlterUserStatement(string userName, IAlterUserAction action) {
			UserName = userName ?? throw new ArgumentNullException(nameof(userName));
			Action = action ?? throw new ArgumentNullException(nameof(action));
		}

		public string UserName { get; }

		public IAlterUserAction Action { get; }

		protected override void Require(IRequirementCollection requirements) {
			requirements.Require(x => x.UserIsAdmin());
		}

		protected override async Task ExecuteStatementAsync(StatementContext context) {
			var securityManager = context.GetSecurityManager();
			if (!await securityManager.UserExistsAsync(UserName))
				throw new SqlStatementException($"User {UserName} does not exist");

			try {
				if (!await Action.AlterUserAsync(UserName, context))
					throw new SqlStatementException($"The alter of the user {UserName} failed");

			} catch (SqlStatementException) {
				throw;
			} catch (Exception ex) {
				throw new SqlStatementException($"It was not possible to alter user {UserName} because of an error", ex);
			}
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("ALTER USER ");
			builder.Append(UserName);
			builder.Append(" ");

			Action.AppendTo(builder);
		}
	}
}