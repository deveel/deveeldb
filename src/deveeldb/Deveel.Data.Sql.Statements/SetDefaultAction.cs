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
	public sealed class SetDefaultAction : IAlterTableAction, IPreparable {
		public SetDefaultAction(string columnName, SqlExpression defaultExpression) {
			ColumnName = columnName;
			DefaultExpression = defaultExpression;
		}

		private SetDefaultAction(ObjectData data) {
			ColumnName = data.GetString("ColumnName");
			DefaultExpression = data.GetValue<SqlExpression>("Default");
		}

		public string ColumnName { get; private set; }

		public SqlExpression DefaultExpression { get; private set; }

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.SetDefault; }
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var defaultExp = DefaultExpression;
			if (defaultExp != null)
				defaultExp = defaultExp.Prepare(preparer);

			return new SetDefaultAction(ColumnName, defaultExp);
		}

		void ISerializable.GetData(SerializeData data) {
			data.SetValue("ColumnName", ColumnName);
			data.SetValue("Default", DefaultExpression);
		}
	}
}
