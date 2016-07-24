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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AddConstraintAction : AlterTableAction {
		public AddConstraintAction(SqlTableConstraint constraint) {
			if (constraint == null)
				throw new ArgumentNullException("constraint");

			Constraint = constraint;
		}

		private AddConstraintAction(SerializationInfo info, StreamingContext context) {
			Constraint = (SqlTableConstraint) info.GetValue("Constraint", typeof(SqlTableConstraint));
		}

		public SqlTableConstraint Constraint { get; private set; }

		protected override AlterTableAction PrepareExpressions(IExpressionPreparer preparer) {
			var constraint = (SqlTableConstraint) (Constraint as IPreparable).Prepare(preparer);
			return new AddConstraintAction(constraint);
		}

		protected override AlterTableActionType ActionType {
			get { return AlterTableActionType.AddConstraint; }
		}

		protected override void GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Constraint", Constraint);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			string constraintType;
			if (Constraint.ConstraintType == ConstraintType.ForeignKey) {
				constraintType = "FOREIGN KEY";
			} else if (Constraint.ConstraintType == ConstraintType.PrimaryKey) {
				constraintType = "PRIMARY KEY";
			} else {
				constraintType = Constraint.ConstraintType.ToString().ToUpperInvariant();
			}

			builder.Append("ADD CONSTRAINT ");
			if (!String.IsNullOrEmpty(Constraint.ConstraintName)) {
				builder.Append(Constraint.ConstraintName);
				builder.Append(" ");
			}

			builder.Append(constraintType);

			if (Constraint.Columns != null &&
				Constraint.Columns.Length > 0)
				builder.AppendFormat("({0})", String.Join(", ", Constraint.Columns));

			if (Constraint.ConstraintType == ConstraintType.ForeignKey) {
				string onDelete = Constraint.OnDelete.AsSqlString();
				string onUpdate = Constraint.OnUpdate.AsSqlString();

				builder.AppendFormat(" REFERENCES {0}({1}) ON DELETE {2} ON UPDATE {3}", Constraint.ReferenceTable,
					String.Join(", ", Constraint.ReferenceColumns), onDelete, onUpdate);
			} else if (Constraint.ConstraintType == ConstraintType.Check) {
				builder.AppendFormat(" {0}", Constraint.CheckExpression);
			}
		}
	}
}
