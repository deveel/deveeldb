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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlQueryExpression : SqlExpression {
		public SqlQueryExpression(IEnumerable<SelectColumn> selectColumns) {
			SelectColumns = selectColumns;
			FromClause = new FromClause();
		}

		private SqlQueryExpression(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			SelectColumns = (SelectColumn[]) info.GetValue("Columns", typeof (SelectColumn[]));

			Distinct = info.GetBoolean("Distinct");
			FromClause = (FromClause)info.GetValue("From", typeof(FromClause));
			WhereExpression = (SqlExpression)info.GetValue("Where", typeof(SqlExpression));
			HavingExpression = (SqlExpression) info.GetValue("Having", typeof(SqlExpression));
			GroupBy = (SqlExpression[])info.GetValue("GroupBy", typeof(SqlExpression[]));
			GroupMax = (ObjectName) info.GetValue("GroupMax", typeof(ObjectName));
			NextComposite = (SqlQueryExpression) info.GetValue("NextComposite", typeof (SqlQueryExpression));

			CompositeFunction = (CompositeFunction) info.GetInt32("CompositeFunction");
			IsCompositeAll = info.GetBoolean("CompositeAll");
		}

		public IEnumerable<SelectColumn> SelectColumns { get; private set; }

		public FromClause FromClause { get; set; }

		public SqlExpression WhereExpression { get; set; }

		public SqlExpression HavingExpression { get; set; }

		public IEnumerable<SqlExpression> GroupBy { get; set; }

		public ObjectName GroupMax { get; set; }

		public SqlQueryExpression NextComposite { get; set; }

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Query; }
		}

		public override bool CanEvaluate {
			get { return true; }
		}

		public CompositeFunction CompositeFunction { get; set; }

		public bool IsCompositeAll { get; set; }

		public bool Distinct { get; set; }

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			if (SelectColumns != null) {
				info.AddValue("Columns", SelectColumns.ToArray());
			} else {
				info.AddValue("Columns", null, typeof(SelectColumn[]));
			}

			info.AddValue("Distinct", Distinct);
			info.AddValue("From", FromClause, typeof(FromClause));
			info.AddValue("Where", WhereExpression, typeof(SqlExpression));
			info.AddValue("Having", HavingExpression, typeof(SqlExpression));

			if (GroupBy != null) {
				info.AddValue("GroupBy", GroupBy.ToArray());
			} else {
				info.AddValue("GroupBy", null, typeof(SqlExpression[]));
			}

			info.AddValue("GroupMax", GroupMax, typeof(ObjectName));
			info.AddValue("NextComposite", NextComposite, typeof(SqlQueryExpression));
			info.AddValue("CompositeFunction", (int)CompositeFunction);
			info.AddValue("CompositeAll", IsCompositeAll);
		}
	}
}