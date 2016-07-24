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
	public sealed class DropColumnAction : AlterTableAction {
		public DropColumnAction(string columnName) {
			ColumnName = columnName;
		}

		private DropColumnAction(SerializationInfo info, StreamingContext context) {
			ColumnName = info.GetString("Column");
		}

		public string ColumnName { get; private set; }

		protected override AlterTableActionType ActionType {
			get { return AlterTableActionType.DropColumn; }
		}

		protected override void GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Column", ColumnName);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.AppendFormat("DROP COLUMN {0}", ColumnName);
		}
	}
}
