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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlQueryExpression : SqlExpression,ISqlExpressionPreparable<SqlQueryExpression> {
		public SqlQueryExpression()
			: base(SqlExpressionType.Query) {
			From = new SqlQueryExpressionFrom();
			GroupBy = new List<SqlExpression>();
			Items = new ItemList();
		}

		public IList<SqlQueryExpressionItem> Items { get; }

		public bool AllItems {
			get => Items.Count == 1 && Items[0].IsAll;
			set {
				if (value) {
					Items.Add(SqlQueryExpressionItem.All);
				}
			}
		}

		public SqlQueryExpressionFrom From { get; set; }

		public override bool CanReduce => true;

		public bool Distinct { get; set; }

		public SqlExpression Where { get; set; }

		public SqlExpression Having { get; set; }

		public IList<SqlExpression> GroupBy { get; set; }

		public ObjectName GroupMax { get; set; }

	    public SqlQueryExpressionComposite NextComposite { get; set; }

        public override SqlType GetSqlType(QueryContext context) {
			throw new NotImplementedException();
		}

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitQuery(this);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("SELECT ");
			if (Distinct)
				builder.Append("DISTINCT ");

			for (int i = 0; i < Items.Count; i++) {
				Items[i].AppendTo(builder);

				if (i < Items.Count - 1)
					builder.Append(", ");
			}

			if (!From.IsEmpty) {
				builder.AppendLine();
				builder.Indent();
				From.AppendTo(builder);

				if (Where != null) {
					builder.AppendLine();
					builder.Append("WHERE ");
					Where.AppendTo(builder);
				} else if (Having != null) {
					builder.AppendLine();
					builder.Append("HAVING ");
					Having.AppendTo(builder);
				}
			}

			// TODO: continue
		}

		SqlQueryExpression ISqlExpressionPreparable<SqlQueryExpression>.Prepare(ISqlExpressionPreparer preparer) {
			var query = new SqlQueryExpression {
				GroupMax = GroupMax,
				Distinct = Distinct,
			};

			foreach (var item in Items) {
				var preparedItem = item.Prepare(preparer);
				query.Items.Add(preparedItem);
			}

			var from = this.From;
			if (from != null)
				from = from.Prepare(preparer);

			query.From = from;

			var where = Where;
			if (where != null)
				where = where.Prepare(preparer);

			query.Where = where;

			var having = Having;
			if (having != null)
				having = having.Prepare(preparer);

			query.Having = having;

			if (GroupBy != null) {
				query.GroupBy = new List<SqlExpression>();

				foreach (var groupByItem in GroupBy) {
					var exp = groupByItem.Prepare(preparer);
					query.GroupBy.Add(exp);
				}
			}

			return query;
		}

		#region ItemList

		class ItemList : Collection<SqlQueryExpressionItem> {
			protected override void InsertItem(int index, SqlQueryExpressionItem item) {
				if (item.IsAll && Items.Any(x => x.IsAll))
					throw new ArgumentException("A query expression cannot contain more than one ALL item");

				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, SqlQueryExpressionItem item) {
				if (item.IsAll) {
					var other = Items[index];
					if (!other.IsAll)
						throw new ArgumentException("Trying to set an ALL item in a query that has already one.");
				}

				base.SetItem(index, item);
			}

			protected override void RemoveItem(int index) {
				if (Items.Count - 1 == 0)
					throw new InvalidOperationException("Cannot remove the last item of a select list");

				base.RemoveItem(index);
			}
		}

		#endregion
	}
}