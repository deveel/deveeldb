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
using System.IO;

using Deveel.Data;
using Deveel.Data.Security;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateUserStatement : SqlStatement {
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

		protected override bool IsPreparable {
			get { return true; }
		}

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var preparedPassword = Password.Prepare(preparer);
			return new CreateUserStatement(UserName, preparedPassword);
		}

		protected override ITable ExecuteStatement(IRequest context) {
			var evaluated = Password.EvaluateToConstant(context, null);
			var passwordText = evaluated.AsVarChar().Value.ToString();

			context.Query.CreateUser(UserName, passwordText);
			return FunctionTable.ResultTable(context, 0);
		}

		#region PreparedSerializer

		internal class PreparedSerializer : ObjectBinarySerializer<CreateUserStatement> {
			public override void Serialize(CreateUserStatement obj, BinaryWriter writer) {
				writer.Write(obj.UserName);
				SqlExpression.Serialize(obj.Password, writer);
			}

			public override CreateUserStatement Deserialize(BinaryReader reader) {
				var userName = reader.ReadString();
				var expression = SqlExpression.Deserialize(reader);

				return new CreateUserStatement(userName, expression);
			}
		}

		#endregion
	}
}
