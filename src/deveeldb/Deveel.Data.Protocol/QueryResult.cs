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
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;

namespace Deveel.Data.Protocol {
	public sealed class QueryResult : IDisposable {
		/// <summary>
		/// The SqlQuery that was executed to produce this result.
		/// </summary>
		private SqlQuery query;

		/// <summary>
		/// The table that is the result.
		/// </summary>
		private Table result;

		/// <summary>
		/// A set of ColumnDescription that describes each column in the ResultSet.
		/// </summary>
		private ColumnDescription[] colDesc;

		/// <summary>
		/// The <see cref="IList{T}"/> that contains the row index into the table 
		/// for each row of the result.
		/// </summary>
		private IList<int> rowIndexMap;

		/// <summary>
		/// Set to true if the result table has a <see cref="SimpleRowEnumerator"/>, therefore 
		/// guarenteeing we do not need to store a row lookup list.
		/// </summary>
		private readonly bool resultIsSimpleEnum;

		/// <summary>
		/// The number of rows in the result.
		/// </summary>
		private readonly int resultRowCount;

		/// <summary>
		/// Incremented when we Lock roots.
		/// </summary>
		private int locked;

		/// <summary>
		/// A <see cref="Dictionary{TKey,TValue}"/> of blob_reference_id values to <see cref="IRef"/> 
		/// objects used to handle and streamable objects in this result.
		/// </summary>
		private readonly Dictionary<long, StreamableObject> streamableBlobMap;


		/// <summary>
		/// Constructs the result set.
		/// </summary>
		/// <param name="query"></param>
		/// <param name="result"></param>
		public QueryResult(SqlQuery query, Table result) {
			this.query = query;
			this.result = result;
			streamableBlobMap = new Dictionary<long, StreamableObject>();

			resultRowCount = result.RowCount;

			// HACK: Read the contents of the first row so that we can pick up
			//   any errors with reading, and also to fix the 'uniquekey' bug
			//   that causes a new transaction to be started if 'uniquekey' is
			//   a column and the value is resolved later.
			IRowEnumerator rowEnum = result.GetRowEnumerator();
			if (rowEnum.MoveNext()) {
				int rowIndex = rowEnum.RowIndex;
				for (int c = 0; c < result.ColumnCount; ++c) {
					result.GetCell(c, rowIndex);
				}
			}

			// If simple enum, note it here
			resultIsSimpleEnum = (rowEnum is SimpleRowEnumerator);
			rowEnum = null;

			// Build 'row_index_map' if not a simple enum
			if (!resultIsSimpleEnum) {
				rowIndexMap = new List<int>(result.RowCount);

				IRowEnumerator en = result.GetRowEnumerator();
				while (en.MoveNext()) {
					rowIndexMap.Add(en.RowIndex);
				}
			}

			// This is a safe operation provides we are shared.
			// Copy all the TableField columns from the table to our own
			// ColumnDescription array, naming each column by what is returned from
			// the 'GetResolvedVariable' method.
			int colCount = result.ColumnCount;
			colDesc = new ColumnDescription[colCount];
			for (int i = 0; i < colCount; ++i) {
				VariableName v = result.GetResolvedVariable(i);
				string fieldName;
				if (v.TableName == null) {
					// This means the column is an alias
					fieldName = String.Format("@a{0}", v.Name);
				} else {
					// This means the column is an schema/table/column reference
					fieldName = String.Format("@f{0}", v);
				}

				colDesc[i] = new ColumnDescription(fieldName, result.GetColumnInfo(i));
			}

			locked = 0;
		}

		/// <summary>
		/// Returns a <see cref="IRef"/> that has been cached in this table object 
		/// by its identifier value.
		/// </summary>
		/// <param name="id"></param>
		/// <returns></returns>
		public StreamableObject GetRef(long id) {
			StreamableObject reference;
			if (!streamableBlobMap.TryGetValue(id, out reference))
				return null;

			return reference;
		}

		/// <summary>
		/// Removes a <see cref="IRef"/> that has been cached in this table object 
		/// by its identifier value.
		/// </summary>
		/// <param name="id"></param>
		public void RemoveRef(long id) {
			streamableBlobMap.Remove(id);
		}

		/// <summary>
		/// Disposes this object.
		/// </summary>
		public void Dispose() {
			while (locked > 0) {
				UnlockRoot(-1);
			}
			result = null;
			rowIndexMap = null;
			colDesc = null;
		}

		/// <summary>
		/// Gets the cell contents of the cell at the given row/column.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="row"></param>
		/// <remarks>
		/// Safe only if roots are locked.
		/// </remarks>
		/// <returns></returns>
		public TObject GetCellContents(int column, int row) {
			if (locked <= 0)
				throw new Exception("Table roots not locked!");

			int realRow = resultIsSimpleEnum ? row : rowIndexMap[row];
			TObject tob = result.GetCell(column, realRow);

			// If this is a large object reference then cache it so a streamable
			// object can reference it via this result.
			if (tob.Object is IRef) {
				var reference = (IRef) tob.Object;
				streamableBlobMap[reference.Id] = 
					new StreamableObject(reference.Type, reference.RawSize, reference.Id);
			}

			return tob;
		}

		/// <summary>
		/// Returns the column count.
		/// </summary>
		public int ColumnCount {
			get { return result.ColumnCount; }
		}

		/// <summary>
		/// Returns the row count.
		/// </summary>
		public int RowCount {
			get { return resultRowCount; }
		}

		/// <summary>
		/// Returns the ColumnDescription array of all the columns in the result.
		/// </summary>
		public ColumnDescription[] Fields {
			get { return colDesc; }
		}

		/// <summary>
		/// Locks the root of the result set.
		/// </summary>
		/// <param name="key"></param>
		public void LockRoot(int key) {
			result.LockRoot(key);
			++locked;
		}

		/// <summary>
		/// Unlocks the root of the result set.
		/// </summary>
		/// <param name="key"></param>
		private void UnlockRoot(int key) {
			result.UnlockRoot(key);
			--locked;
		}
	}
}