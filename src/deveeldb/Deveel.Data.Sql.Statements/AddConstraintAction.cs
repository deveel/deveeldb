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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AddConstraintAction : IAlterTableAction, IPreparable {
		public AddConstraintAction(SqlTableConstraint constraint) {
			if (constraint == null)
				throw new ArgumentNullException("constraint");

			Constraint = constraint;
		}

		private AddConstraintAction(ObjectData data) {
			Constraint = data.GetValue<SqlTableConstraint>("Constraint");
		}

		public SqlTableConstraint Constraint { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var constraint = (SqlTableConstraint) (Constraint as IPreparable).Prepare(preparer);
			return new AddConstraintAction(constraint);
		}

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.AddConstraint; }
		}

		void ISerializable.GetData(SerializeData data) {
			data.SetValue("Constraint", Constraint);
		}
	}
}
