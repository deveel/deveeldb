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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SqlTableColumn : IPreparable {
		public SqlTableColumn(string columnName, DataType columnType) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");
			if (columnType == null)
				throw new ArgumentNullException("columnType");
			
			ColumnName = columnName;
			ColumnType = columnType;
		}

		public string ColumnName { get; private set; }

		public DataType ColumnType { get; private set; }

		public bool IsIdentity { get; set; }

		public SqlExpression DefaultExpression { get; set; }

		public bool HasDefaultExpression {
			get { return DefaultExpression != null; }
		}

		public bool IsNotNull { get; set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var column = new SqlTableColumn(ColumnName, ColumnType);
			if (DefaultExpression != null)
				column.DefaultExpression = DefaultExpression.Prepare(preparer);

			column.IsNotNull = IsNotNull;
			return column;
		}
	}
}
