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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Tables {
	public static class MutableTableExtensions {
		public static bool RemoveRow(this IMutableTable table, int rowIndex) {
			return table.RemoveRow(new RowId(table.TableInfo.Id, rowIndex));
		}

		public static void DeleteRows(this IMutableTable table, IEnumerable<int> rows) {
			foreach (var row in rows) {
				table.RemoveRow(row);
			}
		}

		/// <summary>
		/// Creates a new row that is compatible with the 
		/// table context, ready to be populated and added.
		/// </summary>
		/// <remarks>
		/// <para>
		/// When this method is called, a new <see cref="RowId"/>
		/// is generated and persisted: when a subsequent call to
		/// this method will be issued, another new row identifier
		/// will be generated, even if the row was not persisted
		/// into the table.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="Row"/> that
		/// belongs to this table and can be added or updated through
		/// <see cref="IMutableTable.AddRow"/> or <see cref="IMutableTable.UpdateRow"/> 
		/// method calls.
		/// </returns>
		public static Row NewRow(this IMutableTable table) {
			return new Row(table);
		}

		public static int Delete(this IMutableTable table, ITable t) {
			return Delete(table, t, -1);
		}

		public static int Delete(this IMutableTable table, ITable other, int limit) {
			List<int> rowSet = new List<int>(other.RowCount);
			var e = other.GetEnumerator();
			while (e.MoveNext()) {
				rowSet.Add(e.Current.RowId.RowNumber);
			}

			// HACKY: Find the first column of this table in the search table.  This
			//   will allow us to generate a row set of only the rows in the search
			//   table.
			int firstColumn = other.IndexOfColumn(table.GetResolvedColumnName(0));

			if (firstColumn == -1)
				throw new DatabaseSystemException("Search table does not contain any reference to table being deleted from");

			// Generate a row set that is in this tables domain.
			var rowsToDelete = other.ResolveRows(firstColumn, rowSet, table).ToList();

			// row_set may contain duplicate row indices, therefore we must sort so
			// any duplicates are grouped and therefore easier to find.
			rowSet.Sort();

			// If limit less than zero then limit is whole set.
			if (limit < 0)
				limit = Int32.MaxValue;

			// Remove each row in row set in turn.  Make sure we don't remove the
			// same row index twice.
			int len = System.Math.Min(rowsToDelete.Count, limit);
			int lastRemoved = -1;
			int removeCount = 0;
			for (int i = 0; i < len; ++i) {
				int toRemove = rowsToDelete[i];
				if (toRemove < lastRemoved)
					throw new DatabaseSystemException("Internal error: row sorting error or row set not in the range > 0");

				if (toRemove != lastRemoved) {
					table.RemoveRow(toRemove);
					lastRemoved = toRemove;
					++removeCount;
				}
			}

			if (removeCount > 0)
				// Perform a referential integrity check on any changes to the table.
				table.AssertConstraints();

			return removeCount;
		}

		public static bool Delete(this IMutableTable table, int columnOffset, DataObject value) {
			var list = table.SelectRowsEqual(columnOffset, value).ToArray();
			if (list.Length == 0)
				return false;

			return table.RemoveRow(list[0]);
		}

		public static int Update(this IMutableTable mutableTable, IQueryContext context, ITable table,
			IEnumerable<SqlAssignExpression> assignList, int limit) {

			// Get the rows from the input table.
			var rowSet = new List<int>();
			var e = table.GetEnumerator();
			while (e.MoveNext()) {
				rowSet.Add(e.Current.RowId.RowNumber);
			}

			// HACKY: Find the first column of this table in the search table.  This
			//   will allow us to generate a row set of only the rows in the search
			//   table.
			int firstColumn = table.FindColumn(mutableTable.GetResolvedColumnName(0));
			if (firstColumn == -1)
				throw new InvalidOperationException("Search table does not contain any " +
											"reference to table being updated from");

			// Convert the row_set to this table's domain.
			rowSet = table.ResolveRows(firstColumn, rowSet, mutableTable).ToList();

			// NOTE: Assume there's no duplicate rows.

			// If limit less than zero then limit is whole set.
			if (limit < 0)
				limit = Int32.MaxValue;

			// Update each row in row set in turn up to the limit.
			int len = System.Math.Min(rowSet.Count, limit);

			int updateCount = 0;
			for (int i = 0; i < len; ++i) {
				int toUpdate = rowSet[i];

				// Make a row object from this row (plus keep the original intact
				// in case we need to roll back to it).
				var row = mutableTable.GetRow(toUpdate);
				row.SetFromTable();

				// Run each assignment on the row.
				foreach (var assignment in assignList) {
					row.EvaluateAssignment(assignment, context);
				}

				// Update the row
				mutableTable.UpdateRow(row);

				++updateCount;
			}

			if (updateCount > 0)
				// Perform a referential integrity check on any changes to the table.
				mutableTable.AssertConstraints();

			return updateCount;
		}
	}
}