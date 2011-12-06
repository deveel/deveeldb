// 
//  Copyright 2010-2011  Deveel
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

using Deveel.Data.Collections;
using Deveel.Diagnostics;

namespace Deveel.Data {
	public abstract partial class Table {
		/// <summary>
		/// A simple join operation.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <param name="columnName"></param>
		/// <param name="op"></param>
		/// <param name="expression"></param>
		/// <remarks>
		/// A simple join operation is one that has a single joining operator, 
		/// a <see cref="VariableName"/> on the lhs and a simple expression on the 
		/// expression that includes only columns in the expression table. For example, 
		/// <c>id = part_id</c> or <c>id == part_id * 2</c> or <c>id == part_id + vendor_id * 2</c>
		/// <para>
		/// It is important to understand how this algorithm works because all
		/// optimization of the expression must happen before the method starts.
		/// </para>
		/// <para>
		/// The simple join algorithm works as follows:  Every row of the right hand
		/// side table 'table' is iterated through.  The select opreation is applied
		/// to this table given the result evaluation.  Each row that matches is
		/// included in the result table.
		/// </para>
		/// <para>
		/// For optimal performance, the expression should be arranged so that the expression
		/// table is the smallest of the two tables (because we must iterate through
		/// all rows of this table).  This table should be the largest.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table SimpleJoin(IQueryContext context, Table table, VariableName columnName, Operator op, Expression expression) {
			// Find the row with the name given in the condition.
			int lhsColumn = FindFieldName(columnName);

			if (lhsColumn == -1)
				throw new ArgumentException("Unable to find the LHS column specified in the condition: " + columnName, "columnName");

			// Create a variable resolver that can resolve columns in the destination
			// table.
			TableVariableResolver resolver = table.GetVariableResolver();

			// The join algorithm.  It steps through the RHS expression, selecting the
			// cells that match the relation from the LHS table (this table).

			IntegerVector thisRowSet = new IntegerVector();
			IntegerVector tableRowSet = new IntegerVector();

			IRowEnumerator e = table.GetRowEnumerator();

			while (e.MoveNext()) {
				int rowIndex = e.RowIndex;
				resolver.SetId = rowIndex;

				// Resolve expression into a constant.
				TObject expressionValue = expression.Evaluate(resolver, context);

				// Select all the rows in this table that match the joining condition.
				IntegerVector selectedSet = SelectRows(lhsColumn, op, expressionValue);

				// Include in the set.
				int size = selectedSet.Count;
				for (int i = 0; i < size; ++i)
					tableRowSet.AddInt(rowIndex);

				thisRowSet.Append(selectedSet);
			}

			// Create the new VirtualTable with the joined tables.

			Table[] tabs = new Table[] { this, table };
			IntegerVector[] rowSets = new IntegerVector[] { thisRowSet, tableRowSet };

			VirtualTable outTable = new VirtualTable(tabs);
			outTable.Set(tabs, rowSets);

#if DEBUG
			if (Debug.IsInterestedIn(DebugLevel.Information)) {
				Debug.Write(DebugLevel.Information, this,
							outTable + " = " + this + ".SimpleJoin(" + table +
							", " + columnName + ", " + op + ", " + expression + " )");
			}
#endif

			return outTable;

		}

		/// <summary>
		/// Performs a natural join of this table with the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="quick"></param>
		/// <remarks>
		///  This is the same as calling the <see cref="SimpleJoin"/> with no 
		/// conditional.
		/// </remarks>
		/// <returns></returns>
		public Table Join(Table table, bool quick) {
			Table outTable;

			if (quick) {
				// This implementation doesn't materialize the join
				outTable = new NaturallyJoinedTable(this, table);
			} else {

				Table[] tabs = new Table[2];
				tabs[0] = this;
				tabs[1] = table;
				IntegerVector[] rowSets = new IntegerVector[2];

				// Optimized trivial case, if either table has zero rows then result of
				// join will contain zero rows also.
				if (RowCount == 0 || table.RowCount == 0) {

					rowSets[0] = new IntegerVector(0);
					rowSets[1] = new IntegerVector(0);
				} else {
					// The natural join algorithm.
					IntegerVector thisRowSet = new IntegerVector();
					IntegerVector tableRowSet = new IntegerVector();

					// Get the set of all rows in the given table.
					IntegerVector tableSelectedSet = new IntegerVector();
					IRowEnumerator e = table.GetRowEnumerator();
					while (e.MoveNext()) {
						int rowIndex = e.RowIndex;
						tableSelectedSet.AddInt(rowIndex);
					}

					int tableSelectedSetSize = tableSelectedSet.Count;

					// Join with the set of rows in this table.
					e = GetRowEnumerator();
					while (e.MoveNext()) {
						int rowIndex = e.RowIndex;
						for (int i = 0; i < tableSelectedSetSize; ++i) {
							thisRowSet.AddInt(rowIndex);
						}
						tableRowSet.Append(tableSelectedSet);
					}

					// The row sets we are joining from each table.
					rowSets[0] = thisRowSet;
					rowSets[1] = tableRowSet;
				}

				// Create the new VirtualTable with the joined tables.
				VirtualTable virtTable = new VirtualTable(tabs);
				virtTable.Set(tabs, rowSets);

				outTable = virtTable;
			}

#if DEBUG
			if (Debug.IsInterestedIn(DebugLevel.Information))
				Debug.Write(DebugLevel.Information, this, outTable + " = " + this + ".NaturalJoin(" + table + " )");
#endif

			return outTable;
		}

		/// <summary>
		/// Performs a natural join of this table with the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		///  This is the same as calling the <see cref="SimpleJoin"/> with no 
		/// conditional.
		/// </remarks>
		/// <returns></returns>
		public Table Join(Table table) {
			return Join(table, true);
		}

		/// <summary>
		/// Finds all rows in this table that are <i>outside</i> the result
		/// in the given table.
		/// </summary>
		/// <param name="rtable">The right table that must be a decendent of 
		/// this table.</param>
		/// <remarks>
		/// Performs a normal join, then determines unmatched joins.
		/// <para>
		/// It is possible to create an OuterTable with this result to make 
		/// the completed table.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public VirtualTable Outside(Table rtable) {
			// Form the row list for right hand table,
			IntegerVector rowList = new IntegerVector(rtable.RowCount);
			IRowEnumerator e = rtable.GetRowEnumerator();
			while (e.MoveNext()) {
				rowList.AddInt(e.RowIndex);
			}

			int colIndex = rtable.FindFieldName(GetResolvedVariable(0));
			rtable.SetToRowTableDomain(colIndex, rowList, this);

			// This row set
			IntegerVector thisTableSet = new IntegerVector(RowCount);
			e = GetRowEnumerator();
			while (e.MoveNext()) {
				thisTableSet.AddInt(e.RowIndex);
			}

			// 'rowList' is now the rows in this table that are in 'rtable'.
			// Sort both 'thisTableSet' and 'rowList'
			thisTableSet.QuickSort();
			rowList.QuickSort();

			// Find all rows that are in 'thisTableSet' and not in 'row_list'
			IntegerVector resultList = new IntegerVector(96);
			int size = thisTableSet.Count;
			int rowListIndex = 0;
			int row_list_size = rowList.Count;
			for (int i = 0; i < size; ++i) {
				int thisVal = thisTableSet[i];
				if (rowListIndex < row_list_size) {
					int inVal = rowList[rowListIndex];
					if (thisVal < inVal) {
						resultList.AddInt(thisVal);
					} else if (thisVal == inVal) {
						while (rowListIndex < row_list_size &&
							   rowList[rowListIndex] == inVal) {
							++rowListIndex;
						}
					} else {
						throw new ApplicationException("'thisVal' > 'inVal'");
					}
				} else {
					resultList.AddInt(thisVal);
				}
			}

			// Return the new VirtualTable
			VirtualTable table = new VirtualTable(this);
			table.Set(this, resultList);

			return table;
		}

		/// <summary>
		/// Returns a new Table that is the union of the this table and 
		/// the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// A union operation will remove any duplicate rows.
		/// </remarks>
		/// <returns></returns>
		public Table Union(Table table) {
			// Optimizations - handle trivial case of row count in one of the tables
			//   being 0.
			// NOTE: This optimization assumes this table and the unioned table are
			//   of the same type.
			if ((RowCount == 0 && table.RowCount == 0) ||
				table.RowCount == 0) {

#if DEBUG
				if (Debug.IsInterestedIn(DebugLevel.Information))
					Debug.Write(DebugLevel.Information, this, this + " = " + this + ".Union(" + table + " )");
#endif
				return this;
			}
			if (RowCount == 0) {
#if DEBUG
				if (Debug.IsInterestedIn(DebugLevel.Information))
					Debug.Write(DebugLevel.Information, this, table + " = " + this + ".Union(" + table + " )");
#endif
				return table;
			}

			// First we merge this table with the input table.

			RawTableInformation raw1 = ResolveToRawTable(new RawTableInformation());
			RawTableInformation raw2 = table.ResolveToRawTable(new RawTableInformation());

			// This will throw an exception if the table types do not match up.

			raw1.Union(raw2);

			// Now 'raw1' contains a list of uniquely merged rows (ie. the union).
			// Now make it into a new table and return the information.

			Table[] tableList = raw1.GetTables();
			VirtualTable tableOut = new VirtualTable(tableList);
			tableOut.Set(tableList, raw1.GetRows());

#if DEBUG
			if (Debug.IsInterestedIn(DebugLevel.Information))
				Debug.Write(DebugLevel.Information, this, tableOut + " = " + this + ".Union(" + table + " )");
#endif

			return tableOut;
		}
	}
}