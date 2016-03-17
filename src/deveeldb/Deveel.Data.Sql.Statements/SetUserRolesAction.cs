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
using System.Collections.Generic;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SetUserRolesAction : IAlterUserAction {
		public SetUserRolesAction(IEnumerable<SqlExpression> roles) {
			if (roles == null)
				throw new ArgumentNullException("roles");

			Roles = roles;
		}

		private SetUserRolesAction(SerializationInfo info, StreamingContext context) {
			Roles = (SqlExpression[]) info.GetValue("Roles", typeof(SqlExpression[]));
		}

		public IEnumerable<SqlExpression> Roles { get; private set; }

		public AlterUserActionType ActionType {
			get { return AlterUserActionType.SetGroups; }
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Roles", Roles);
		}
	}
}
