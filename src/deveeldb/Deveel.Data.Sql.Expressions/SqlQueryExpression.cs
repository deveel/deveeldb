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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlQueryExpression : SqlExpression {
		public SqlQueryExpression(IEnumerable<SelectColumn> selectColumns) {
			SelectColumns = selectColumns;
			FromClause = new FromClause();
		}

		private SqlQueryExpression(ObjectData data) {
			if (data.HasValue("Columns"))
				SelectColumns = data.GetValue<SelectColumn[]>("Columns");

			Distinct = data.GetBoolean("Distinct");
			FromClause = data.GetValue<FromClause>("From");
			WhereExpression = data.GetValue<SqlExpression>("Where");
			HavingExpression = data.GetValue<SqlExpression>("Having");
			GroupBy = data.GetValue<SqlExpression[]>("GroupBy");
			GroupMax = data.GetValue<ObjectName>("GroupMax");
			NextComposite = data.GetValue<SqlQueryExpression>("NextComposite");

			if (data.HasValue("CompositeFunction"))
				CompositeFunction = (CompositeFunction) data.GetInt32("CompositeFunction");

			IsCompositeAll = data.GetBoolean("CompositeAll");
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

		protected override void GetData(SerializeData data) {
			if (SelectColumns != null)
				data.SetValue("Columns", SelectColumns.ToArray());

			data.SetValue("Distinct", Distinct);
			data.SetValue("From", FromClause);
			data.SetValue("Where", WhereExpression);
			data.SetValue("Having", HavingExpression);

			if (GroupBy != null)
				data.SetValue("GroupBy", GroupBy.ToArray());

			data.SetValue("GroupMax", GroupMax);
			data.SetValue("NextComposite", NextComposite);
			data.SetValue("CompositeFunction", (int)CompositeFunction);
			data.SetValue("CompositeAll", IsCompositeAll);
		}
	}
}