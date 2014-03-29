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

using Deveel.Data.Index;

namespace Deveel.Data.DbSystem {
	public abstract partial class MasterTableDataSource {
		/// <summary>
		/// Creates and returns an <see cref="IIndexSet"/> object that is used 
		/// to create indices for this table source.
		/// </summary>
		/// <remarks>
		/// The <see cref="IIndexSet"/> represents a snapshot of the table and 
		/// the given point in time.
		/// <para>
		/// <b>Note</b> Not synchronized because we synchronize in the 
		/// <see cref="IndexSetStore"/> object.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public abstract IIndexSet CreateIndexSet();

		/// <summary>
		/// Commits changes made to an IndexSet returned by the 
		/// <see cref="CreateIndexSet"/> method.
		/// </summary>
		/// <param name="indexSet"></param>
		/// <remarks>
		/// This method also disposes the IndexSet so it is no longer valid.
		/// </remarks>
		protected abstract void CommitIndexSet(IIndexSet indexSet);

		/// <summary>
		/// Creates a <see cref="SelectableScheme"/> object for the given 
		/// column in this table.
		/// </summary>
		/// <param name="indexSet"></param>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <remarks>
		/// This reads the index from the index set (if there is one) then wraps
		/// it around the selectable schema as appropriate.
		/// <para>
		/// <b>Note</b>: This needs to be deprecated in support of composite 
		/// indexes.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal SelectableScheme CreateSelectableSchemeForColumn(IIndexSet indexSet, ITableDataSource table, int column) {
			lock (this) {
				// What's the type of scheme for this column?
				DataColumnInfo columnInfo = TableInfo[column];

				// If the column isn't indexable then return a BlindSearch object
				if (!columnInfo.IsIndexableType)
					return new BlindSearch(table, column);

				string schemeType = columnInfo.IndexScheme;
				if (schemeType.Equals("InsertSearch")) {
					// Search the TableIndexDef for this column
					DataIndexSetInfo indexSetInfo = IndexSetInfo;
					int indexI = indexSetInfo.FindIndexForColumns(new string[] { columnInfo.Name });
					return CreateSelectableSchemeForIndex(indexSet, table, indexI);
				}

				if (schemeType.Equals("BlindSearch"))
					return new BlindSearch(table, column);

				throw new ApplicationException("Unknown scheme type");
			}
		}

		/// <summary>
		/// Creates a SelectableScheme object for the given index in the index 
		/// set info in this table.
		/// </summary>
		/// <param name="indexSet"></param>
		/// <param name="table"></param>
		/// <param name="indexI"></param>
		/// <remarks>
		/// This reads the index from the index set (if there is one) then 
		/// wraps it around the selectable schema as appropriate.
		/// </remarks>
		/// <returns></returns>
		internal SelectableScheme CreateSelectableSchemeForIndex(IIndexSet indexSet, ITableDataSource table, int indexI) {
			lock (this) {
				// Get the IndexDef object
				DataIndexInfo dataIndexInfo = IndexSetInfo[indexI];

				if (dataIndexInfo.Type.Equals("BLIST")) {
					string[] cols = dataIndexInfo.ColumnNames;
					DataTableInfo dataTableInfo = TableInfo;
					if (cols.Length != 1)
						throw new Exception("Multi-column indexes not supported at this time.");

					// If a single column
					int colIndex = dataTableInfo.FindColumnName(cols[0]);

					// Get the index from the index set and set up the new InsertSearch
					// scheme.
					IIndex indexList = indexSet.GetIndex(dataIndexInfo.Pointer);
					return new InsertSearch(table, colIndex, indexList);
				}

				throw new Exception("Unrecognised type.");
			}
		}

		/// <summary>
		/// Builds a complete index set on the data in this table.
		/// </summary>
		/// <remarks>
		/// This must only be called when either:
		/// <list type="bullet">
		/// <item>we are under a commit lock</item>
		/// <item>there is a guarentee that no concurrect access to the indexing 
		/// information can happen (such as when we are creating the table).</item>
		/// </list>
		/// <para>
		/// <b>Note</b> We assume that the index information for this table is 
		/// blank before this method is called.
		/// </para>
		/// </remarks>
		internal void BuildIndexes() {
			lock (this) {
				IIndexSet indexSet = CreateIndexSet();

				DataIndexSetInfo indexSetInfo = IndexSetInfo;

				int rowCount = RawRowCount;

				// Master index is always on index position 0
				IIndex masterIndex = indexSet.GetIndex(0);

				// First, update the master index
				for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
					// If this row isn't deleted, set the index information for it,
					if (!IsRecordDeleted(rowIndex)) {
						// First add to master inde
						if (!masterIndex.UniqueInsertSort(rowIndex))
							throw new Exception("Assertion failed: Master index entry was duplicated.");
					}
				}

				// Commit the master index
				CommitIndexSet(indexSet);

				// Now go ahead and build each index in this table
				int indexCount = indexSetInfo.IndexCount;
				for (int i = 0; i < indexCount; ++i) {
					BuildIndex(i);
				}
			}
		}

		/// <summary>
		/// Builds the given index number (from the <see cref="IndexSetInfo"/>).
		/// </summary>
		/// <param name="indexNumber"></param>
		/// <remarks>
		/// This must only be called when either:
		/// <list type="bullet">
		/// <item>we are under a commit lock</item>
		/// <item>there is a guarentee that no concurrect access to the indexing 
		/// information can happen (such as when we are creating the table).</item>
		/// </list>
		/// <para>
		/// <b>Note</b> We assume that the index number in this table is blank before this
		/// method is called.
		/// </para>
		/// </remarks>
		internal void BuildIndex(int indexNumber) {
			lock (this) {
				IIndexSet indexSet = CreateIndexSet();

				// Master index is always on index position 0
				IIndex masterIndex = indexSet.GetIndex(0);
				// A minimal ITableDataSource for constructing the indexes
				ITableDataSource minTableSource = GetMinimalTableDataSource(masterIndex);

				// Set up schemes for the index,
				SelectableScheme scheme = CreateSelectableSchemeForIndex(indexSet, minTableSource, indexNumber);

				// Rebuild the entire index
				int rowCount = RawRowCount;
				for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
					// If this row isn't deleted, set the index information for it,
					if (!IsRecordDeleted(rowIndex))
						scheme.Insert(rowIndex);
				}

				// Commit the index
				CommitIndexSet(indexSet);
			}
		}

		/// <summary>
		/// Sets up the <see cref="IndexSetInfo"/> object from the information 
		/// set in this object
		/// </summary>
		/// <remarks>
		/// This will only setup a default <see cref="IndexSetInfo"/> on the 
		/// information in the <see cref="TableInfo"/>.
		/// </remarks>
		private void SetIndexSetInfo() {
			lock (this) {
				// Create the initial DataIndexSetInfo object.
				indexInfo = new DataIndexSetInfo(tableInfo.TableName);
				for (int i = 0; i < tableInfo.ColumnCount; ++i) {
					DataColumnInfo colInfo = tableInfo[i];
					if (colInfo.IsIndexableType &&
						colInfo.IndexScheme.Equals("InsertSearch")) {
						indexInfo.AddIndex(new DataIndexInfo("ANON-COLUMN:" + i, new String[] { colInfo.Name }, i + 1, "BLIST", false));
					}
				}
			}
		}
	}
}