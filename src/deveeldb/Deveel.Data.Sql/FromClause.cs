// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A container for the <i>FROM</i> clause of a select statement.
	/// </summary>
	/// <remarks>
	/// This handles the different types of joins.
	/// </remarks>
	[Serializable]
	public sealed class FromClause : IPreparable {
		internal FromClause() {
		}

		public FromTable SourceTable { get; private set; }

		public IEnumerable<FromTable> AllTables { get; private set; }

		public void SetTable(FromTable table) {
			if (table == null) 
				throw new ArgumentNullException("table");
			if (SourceTable != null)
				throw new InvalidOperationException("");

			SourceTable = table;
		}

		public void SetTable(ObjectName tableName) {
			SetTable(tableName, null);
		}

		public void SetTable(ObjectName tableName, string alias) {
			if (tableName == null) 
				throw new ArgumentNullException("tableName");

			SetTable(new FromTable(tableName.FullName, alias));
		}

		public void SetTable(SqlQueryExpression subQuery) {
			SetTable(subQuery, null);
		}

		public void SetTable(SqlQueryExpression subQuery, string alias) {
			if (subQuery == null) 
				throw new ArgumentNullException("subQuery");

			SetTable(new FromTable(subQuery, alias));
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}
	}
}