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

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateUserStatement : SqlNonPreparableStatement {
		public CreateUserStatement(string userName, SqlExpression password) {
			if (password == null)
				throw new ArgumentNullException("password");
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			UserName = userName;
			Password = password;
		}

		public string UserName { get; private set; }

		public SqlExpression Password { get; private set; }


		public override ITable Execute(IQueryContext context) {
			var passwordText = Password.EvaluateToConstant(context, null).Value.ToString();

			try {
				context.CreateUser(UserName, passwordText);
				return FunctionTable.ResultTable(context, 0);
			} catch (Exception ex) {
				throw new StatementException(String.Format("Could not create user '{0}' because of an error", UserName), ex);
			}
		}
	}
}
