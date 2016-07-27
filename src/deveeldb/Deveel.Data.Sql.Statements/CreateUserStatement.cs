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
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CreateUserStatement : SqlStatement, IPreparable {
		public CreateUserStatement(string userName, SqlUserIdentifier identifier) {
			if (identifier == null)
				throw new ArgumentNullException("identifier");
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			UserName = userName;
			Identifier = identifier;
		}

		private CreateUserStatement(SerializationInfo info, StreamingContext context) {
			UserName = info.GetString("UserName");
			Identifier = (SqlUserIdentifier) info.GetValue("Identifier", typeof(SqlUserIdentifier));
		}

		public string UserName { get; private set; }

		public SqlUserIdentifier Identifier { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("UserName", UserName);
			info.AddValue("Identifier", Identifier);
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var preparedPassword = (SqlUserIdentifier) (Identifier as IPreparable).Prepare(preparer);
			return new CreateUserStatement(UserName, preparedPassword);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (Identifier.Type != SqlIdentificationType.Password)
				throw new NotSupportedException(String.Format("The identification type '{0}' is not supported yet.", Identifier.Type));

			var password = Identifier.Argument;
			var evaluated = password.EvaluateToConstant(context.Request, null);
			var passwordText = evaluated.AsVarChar().Value.ToString();

			if (!context.User.CanManageUsers())
				throw new SecurityException(String.Format("User '{0}' cannot create users.", context.User.Name));

			if (context.DirectAccess.UserExists(UserName))
				throw new SecurityException(String.Format("The user '{0}' already exists.", UserName));
			if (context.DirectAccess.RoleExists(UserName))
				throw new SecurityException(String.Format("A role named '{0}' already exists.", UserName));

			context.DirectAccess.CreateUser(UserName, passwordText);

		}

		#region PreparedSerializer

		//internal class PreparedSerializer : ObjectBinarySerializer<CreateUserStatement> {
		//	public override void Serialize(CreateUserStatement obj, BinaryWriter writer) {
		//		writer.Write(obj.UserName);
		//		SqlExpression.Serialize(obj.Password, writer);
		//	}

		//	public override CreateUserStatement Deserialize(BinaryReader reader) {
		//		var userName = reader.ReadString();
		//		var expression = SqlExpression.Deserialize(reader);

		//		return new CreateUserStatement(userName, expression);
		//	}
		//}

		#endregion
	}
}
