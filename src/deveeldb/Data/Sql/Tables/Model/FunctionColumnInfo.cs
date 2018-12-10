// 
//  Copyright 2010-2018 Deveel
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
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables.Model {
	public sealed class FunctionColumnInfo {
		public FunctionColumnInfo(SqlExpression function, string columnName, SqlType columnType)
			: this(function, columnName, columnType, false) {
		}

		public FunctionColumnInfo(SqlExpression function, string columnName, SqlType columnType, bool reduced) {
			if (function == null)
				throw new ArgumentNullException(nameof(function));
			if (columnType == null)
				throw new ArgumentNullException(nameof(columnType));
			if (String.IsNullOrWhiteSpace(columnName))
				throw new ArgumentNullException(nameof(columnName));

			Function = function;
			IsReduced = reduced;

			ColumnInfo = new ColumnInfo(columnName, columnType);
		}

		public SqlExpression Function { get; }

		public bool IsReduced { get; }

		public ColumnInfo ColumnInfo { get; }

	}
}