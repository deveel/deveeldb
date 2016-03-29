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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AddColumnAction : IAlterTableAction, IPreparable {
		public AddColumnAction(SqlTableColumn column) {
			if (column == null)
				throw new ArgumentNullException("column");

			Column = column;
		}

		private AddColumnAction(SerializationInfo info, StreamingContext context) {
			Column = (SqlTableColumn) info.GetValue("Column", typeof(SqlTableColumn));
		}

		public SqlTableColumn Column { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var newColumn = new SqlTableColumn(Column.ColumnName, Column.ColumnType) {
				IsNotNull = Column.IsNotNull
			};

			var defaultExp = Column.DefaultExpression;
			if (defaultExp != null)
				newColumn.DefaultExpression = defaultExp.Prepare(preparer);

			return new AddColumnAction(newColumn);
		}

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.AddColumn; }
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Column", Column);
		}
	}
}
