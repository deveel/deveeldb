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

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropSequenceStatement : SqlStatement {
		public DropSequenceStatement(ObjectName sequenceName) {
			if (sequenceName == null)
				throw new ArgumentNullException("sequenceName");

			SequenceName = sequenceName;
		}

		public ObjectName SequenceName { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var seqName = context.Access.ResolveObjectName(DbObjectType.Sequence, SequenceName);
			return new DropSequenceStatement(seqName);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.User.CanDrop(DbObjectType.Sequence, SequenceName))
				throw new MissingPrivilegesException(context.User.Name, SequenceName, Privileges.Drop);

			if (!context.DirectAccess.DropObject(DbObjectType.Sequence, SequenceName))
				throw new StatementException(String.Format("Cannot drop sequence '{0}': maybe not found.", SequenceName));

			context.DirectAccess.RevokeAllGrantsOn(DbObjectType.Sequence, SequenceName);
		}
	}
}
