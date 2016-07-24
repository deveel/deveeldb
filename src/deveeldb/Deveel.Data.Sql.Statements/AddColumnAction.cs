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

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AddColumnAction : AlterTableAction {
		public AddColumnAction(SqlTableColumn column) {
			if (column == null)
				throw new ArgumentNullException("column");

			Column = column;
		}

		private AddColumnAction(SerializationInfo info, StreamingContext context) {
			Column = (SqlTableColumn) info.GetValue("Column", typeof(SqlTableColumn));
		}

		public SqlTableColumn Column { get; private set; }

		protected override AlterTableAction PrepareExpressions(IExpressionPreparer preparer) {
			var newColumn = (SqlTableColumn)((IPreparable)Column).Prepare(preparer);
			return new AddColumnAction(newColumn);
		}

		protected override AlterTableAction PrepareStatement(IRequest context) {
			var newColumn = (SqlTableColumn)((IStatementPreparable)Column).Prepare(context);
			return new AddColumnAction(newColumn);
		}

		protected override AlterTableActionType ActionType {
			get { return AlterTableActionType.AddColumn; }
		}

		protected override void GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Column", Column);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.AppendFormat("ADD COLUMN {0} {1}", Column.ColumnName, Column.ColumnType);
			if (Column.IsNotNull)
				builder.Append(" NOT NULL");
			if (Column.HasDefaultExpression) {
				builder.Append(" DEFAULT");
				Column.DefaultExpression.AppendTo(builder);
			}
		}
	}
}
