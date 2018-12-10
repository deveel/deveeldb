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
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables.Model
{
	public sealed class GroupTable : FunctionTable {
		private readonly ITable table;
		private Dictionary<int, BigList<long>> groupLinks;
		private BigArray<int> groupLookup;
		private bool wholeTableAsGroup;
		private BigList<long> wholeTableGroup;
		private long wholeTableGroupSize;
		private bool wholeTableIsSimpleEnum;

		private GroupResolver groupResolver;

		public GroupTable(IContext context, ITable table, FunctionColumnInfo[] columnInfo, ObjectName[] columns)
			: base(context, table, columnInfo) {

			this.table = table;

			if (columns != null && columns.Length > 0) {
				CreateMatrix(columns);
			} else {
				CreateGroup();
			}

			// Set up a group resolver for this method.
			groupResolver = new GroupResolver(this);
		}

		private void CreateGroup() {
			wholeTableAsGroup = true;

			wholeTableGroupSize = table.RowCount;

			// Set up 'whole_table_group' to the list of all rows in the reference
			// table.
			var en = table.GetEnumerator();
			wholeTableIsSimpleEnum = en is SimpleRowEnumerator;
			if (!wholeTableIsSimpleEnum) {
				wholeTableGroup = new BigList<long>(table.RowCount);
				while (en.MoveNext()) {
					wholeTableGroup.Add(en.Current.Number);
				}
			}
		}

		private void CreateMatrix(ObjectName[] columns) {
			var rootTable = table;
			long rowCount = rootTable.RowCount;
			int[] colLookup = new int[columns.Length];
			for (int i = columns.Length - 1; i >= 0; --i) {
				colLookup[i] = rootTable.TableInfo.Columns.IndexOf(columns[i]);
			}

			var rowList = rootTable.OrderRowsByColumns(colLookup).ToBigArray();

			// 'row_list' now contains rows in this table sorted by the columns to
			// group by.

			// This algorithm will generate two lists.  The group_lookup list maps
			// from rows in this table to the group number the row belongs in.  The
			// group number can be used as an index to the 'group_links' list that
			// contains consequtive links to each row in the group until -1 is reached
			// indicating the end of the group;

			groupLookup = new BigArray<int>(rowCount);
			groupLinks = new Dictionary<int, BigList<long>>();
			int currentGroup = 0;
			var groupRows = new BigList<long>(rowCount);
			long previousRow = -1;
			bool lastNotEqual = false;
			for (long i = 0; i < rowCount; i++) {
				var rowIndex = rowList[i];
				if (previousRow != -1) {
					bool equal = true;
					// Compare cell in column in this row with previous row.
					for (int n = 0; n < colLookup.Length && equal; ++n) {
						var c1 = rootTable.GetValue(rowIndex, colLookup[n]);
						var c2 = rootTable.GetValue(previousRow, colLookup[n]);
						equal = (c1.CompareTo(c2) == 0);
					}

					groupRows.Add(previousRow);

					if (!equal) {
						groupLinks[currentGroup]= groupRows;
						lastNotEqual = true;
					} else if (lastNotEqual) {
						currentGroup = groupLinks.Count;
						groupRows = new BigList<long>(rowCount);
						lastNotEqual = false;
					}
				}

				groupLookup[rowIndex] = currentGroup;

				previousRow = rowIndex;
			}

			groupRows.Add(previousRow);
		}

		protected override IQuery CreateQuery(long row) {
			return Context.CreateQuery(groupResolver.GetRowResolver(row), Context.Resolver());
		}

		public override VirtualTable GroupMax(ObjectName maxColumn) {
			IEnumerable<long> rows;

			if (wholeTableAsGroup) {
				// Whole table is group, so take top entry of table.

				var rowList = new BigList<long>(1);
				using (var rowEnum = table.GetEnumerator()) {
					if (rowEnum.MoveNext()) {
						rowList.Add(rowEnum.Current.Number);
					} else {
						// MAJOR HACK: If the referencing table has no elements then we choose
						//   an arbitrary index from the reference table to merge so we have
						//   at least one element in the table.
						//   This is to fix the 'SELECT COUNT(*) FROM empty_table' bug.
						rowList.Add(Int64.MaxValue - 1);
					}
				}

				rows = rowList;
			} else if (table.RowCount == 0) {
				rows = new BigList<long>(0);
			} else if (groupLinks != null) {
				// If we are grouping, reduce down to only include one row from each
				// group.
				if (maxColumn == null) {
					rows = GetTopRowsFromEachGroup();
				} else {
					var colNum = table.TableInfo.Columns.IndexOf(maxColumn);
					rows = GetMaxFromEachGroup(colNum);
				}
			} else {
				// OPTIMIZATION: This should be optimized.  It should be fairly trivial
				//   to generate a Table implementation that efficiently merges this
				//   function table with the reference table.

				// This means there is no grouping, so merge with entire table,
				var rowCount = table.RowCount;
				var rowList = new BigList<long>(rowCount);
				using (var en = table.GetEnumerator()) {
					while (en.MoveNext()) {
						rowList.Add(en.Current.Number);
					}
				}

				rows = rowList;
			}

			// Create a virtual table that's the new group table merged with the
			// functions in this...

			var tabs = new[] { table, this };
			var rowSets = new [] { rows, rows };

			return new VirtualTable(tabs, rowSets);
		}

		private IEnumerable<long> GetTopRowsFromEachGroup() {
			var extractRows = new BigList<long>();
			var size = groupLinks.Count;

			for (int i = 0; i < size; ++i) {
				var r = groupLinks[i];
				extractRows.Add(r[0]);
			}

			return extractRows;
		}

		private IEnumerable<long> GetMaxFromEachGroup(int colNum) {
			var refTab = table;

			var extractRows = new BigList<long>();
			var size = groupLinks.Count;

			for (int i = 0; i < size; ++i) {
				var group = groupLinks[i];
				SqlObject max = null;
				long toTakeInGroup = -1;

				for (int j = 0; j < group.Count; j++) {
					var groupRow = group[j];
					var value = refTab.GetValue(groupRow, colNum);

					if (max == null || value.CompareTo(max) > 0) {
						max = value;
						toTakeInGroup = groupRow;
					}
				}

				extractRows.Add(toTakeInGroup);
			}

			return extractRows;
		}

		private long GetGroupSize(int groupNumber) {
			var group = groupLinks[groupNumber];
			return group.Count;
		}

		private BigList<long> GetGroupRows(int groupNumber) {
			return groupLinks[groupNumber];
		}

		private int GetRowGroup(long rowIndex) {
			return groupLookup[rowIndex];
		}

		#region GroupResolver

		class GroupResolver : IGroupResolver {
			private BigList<long> group;
			private readonly GroupTable table;

			private GroupResolver(GroupTable table, int groupId) {
				this.table = table;
				GroupId = groupId;
			}

			public GroupResolver(GroupTable table)
				: this(table, -1) {
			}

			public long Size {
				get {
					if (GroupId == -2)
						return table.wholeTableGroupSize;
					if (group != null)
						return group.Count;

					return table.GetGroupSize(GroupId);
				}
			}

			public int GroupId { get; }

			private void EnsureGroup() {
				if (group == null) {
					if (GroupId == -2) {
						group = table.wholeTableGroup;
					} else {
						group = table.GetGroupRows(GroupId);
					}
				}
			}

			public IGroupResolver GetRowResolver(long row) {
				if (table.wholeTableAsGroup) {
					return new GroupResolver(table, -2);
				}

				var groupId = table.GetRowGroup(row);
				return new GroupResolver(table, groupId);
			}

			public Task<SqlObject> ResolveReferenceAsync(ObjectName reference, long index) {
				int colIndex = table.table.TableInfo.Columns.IndexOf(reference);
				if (colIndex == -1)
					throw new InvalidOperationException($"Column {reference} not found in table {table.table.TableInfo.TableName}.");

				EnsureGroup();

				var rowIndex = index;
				if (group != null)
					rowIndex = group[index];

				return table.table.GetValueAsync(rowIndex, colIndex);
			}

			public IReferenceResolver GetResolver(long index) {
				return new ReferenceResolver(this, index);
			}

			#region ReferenceResolver

			class ReferenceResolver : IReferenceResolver {
				private readonly GroupResolver groupResolver;
				private readonly long index;

				public ReferenceResolver(GroupResolver groupResolver, long index) {
					this.groupResolver = groupResolver;
					this.index = index;
				}

				public Task<SqlObject> ResolveReferenceAsync(ObjectName referenceName) {
					return groupResolver.ResolveReferenceAsync(referenceName, index);
				}

				public SqlType ResolveType(ObjectName referenceName) {
					var columnOffset = groupResolver.table.TableInfo.Columns.IndexOf(referenceName);
					if (columnOffset < 0)
						throw new InvalidOperationException($"Cannot find column {referenceName} in table {groupResolver.table.TableInfo.TableName}");

					return groupResolver.table.TableInfo.Columns[columnOffset].ColumnType;
				}
			}

			#endregion
		}

		#endregion
	}
}
