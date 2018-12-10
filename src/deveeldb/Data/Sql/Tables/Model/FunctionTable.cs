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
using System.Threading.Tasks;

using Deveel.Data.Query;
using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Tables.Model
{
	public class FunctionTable : DataTableBase {
		private readonly ITable table;
		private int uniqueId;
		private readonly FunctionColumnInfo[] columns;

		private static int uniqueKeySeq = 0;
		public static readonly ObjectName Name = new ObjectName("FUNCTIONTABLE");

		public FunctionTable(IContext context, ITable table, FunctionColumnInfo[] columns) {
			// Make sure we are synchronized over the class.
			lock (typeof(FunctionTable)) {
				uniqueId = uniqueKeySeq;
				++uniqueKeySeq;
			}

			uniqueId = (uniqueId & 0x0FFFFFFF) | 0x010000000;

			var tableInfo = new TableInfo(Name);

			for (int i = 0; i < columns.Length; i++) {
				tableInfo.Columns.Add(columns[i].ColumnInfo);
			}

			TableInfo = tableInfo;
			this.columns = columns;

			this.table = table;
			RowCount = table.RowCount;

			Context = context;
		}

		public override TableInfo TableInfo { get; }

		public override long RowCount { get; }

		protected IContext Context { get; }

		private ITableFieldCache FieldCache => Context.Scope.Resolve<ITableFieldCache>();

		protected virtual IQuery CreateQuery(long row) {
			return Context.CreateQuery(new RowReferenceResolver(table, row));
		}

		public override async Task<SqlObject> GetValueAsync(long row, int column) {
			var cache = FieldCache;
			var expr = columns[column];

			SqlObject value;

			if (cache != null && !expr.IsReduced) {
				if (!cache.TryGetValue(table.TableInfo.TableName, row, column, out value)) {
					value = await GetValueDirect(expr.Function, row);
				}
			} else {
				value = await GetValueDirect(expr.Function, row);
			}

			return value;
		}

		private async Task<SqlObject> GetValueDirect(SqlExpression expression, long row) {
			SqlExpression result;

			using (var context = CreateQuery(row)) {
				result = await expression.ReduceAsync(context);
			}

			if (result.ExpressionType != SqlExpressionType.Constant)
				throw new ArgumentException();

			return ((SqlConstantExpression)result).Value;
		}

		public virtual VirtualTable GroupMax(ObjectName maxColumn) {
			BigList<long> rowList;

			if (table.RowCount <= 0) {
				rowList = new BigList<long>(0);
			} else {
				// OPTIMIZATION: This should be optimized.  It should be fairly trivial
				//   to generate a Table implementation that efficiently merges this
				//   function table with the reference table.

				// This means there is no grouping, so merge with entire table,
				var rowCount = table.RowCount;
				rowList = new BigList<long>(rowCount);
				using (var en = table.GetEnumerator()) {
					while (en.MoveNext()) {
						rowList.Add(en.Current.Number);
					}
				}
			}

			// Create a virtual table that's the new group table merged with the
			// functions in this...

			var tabs = new[] { table, this };
			var rowSets = new IEnumerable<long>[] { rowList, rowList };

			return new VirtualTable(tabs, rowSets);
		}

		public override IEnumerator<Row> GetEnumerator() {
			return new SimpleRowEnumerator(this);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (Context != null)
					Context.Dispose();
			}

			base.Dispose(disposing);
		}
	}
}
