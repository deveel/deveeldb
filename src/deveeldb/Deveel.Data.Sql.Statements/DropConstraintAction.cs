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

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropConstraintAction : AlterTableAction {
		public DropConstraintAction(string constraintName) {
			if (String.IsNullOrEmpty(constraintName))
				throw new ArgumentNullException("constraintName");

			ConstraintName = constraintName;
		}

		private DropConstraintAction(SerializationInfo info, StreamingContext context) {
			ConstraintName = info.GetString("Constraint");
		}

		public string ConstraintName { get; private set; }

		protected override AlterTableActionType ActionType {
			get { return AlterTableActionType.DropConstraint; }
		}

		protected override void GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Constraint", ConstraintName);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.AppendFormat("DROP CONSTRAINT {0}", ConstraintName);
		}
	}
}
