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
using System.IO;

using Deveel.Data.Client;
using Deveel.Data.DbSystem;
using Deveel.Data.Sql;

namespace Deveel.Data.Protocol {
	internal class LocalQueryResult : IDisposable {
		private DeveelDbConnection connection;

		private static int uniqueIdKey = 1;

		private int uniqueId;
		private ColumnDescription[] columns;
		private int queryTimeMs;
		private int resultRowCount;

		private int maxRowCount = Int32.MaxValue;

		private int blockTopRow;
		private int blockRowCount;
		private int fetchSize;

		private Dictionary<string, int> columnHash;

		private int realIndex = Int32.MaxValue;
		private int realIndexOffset = -1;

		private QueryResultPart resultBlock; 

		public LocalQueryResult(DeveelDbConnection connection) {
			this.connection = connection;

			MaxFetchSize = connection.Settings.MaxFetchSize;
			DefaultFetchSize = connection.Settings.FetchSize;
		}

		~LocalQueryResult() {
			Dispose(false);
		}

		public bool VerboseColumnNames {
			get { return connection.Settings.VerboseColumnNames; }
		}

		public int MaxFetchSize { get; private set; }

		public int DefaultFetchSize { get; private set; }

		public bool Closed { get; private set; }

		public int QueryTime { get; internal set; }

		/// <summary>
		/// Returns the identificator that is used as a key to refer to the result set 
		/// on the server that is the result of the command.
		/// </summary>
		/// <remarks>
		/// An identificator of -1 means there is no server side result set associated 
		/// with this object.
		/// </remarks>
		public int ResultId { get; private set; }

		/// <summary>
		/// The total number of rows in the result set.
		/// </summary>
		public int RowCount {
			get {
				// The row count is whatever is the least between max_row_count (the
				// maximum the user has set) and result_row_count (the actual number of
				// rows in the result.
				return System.Math.Min(resultRowCount, maxRowCount);
			}
		}

		/// <summary>
		/// The column count of columns in the result set.
		/// </summary>
		public int ColumnCount {
			get { return columns.Length; }
		}

		public bool HasLargeObject {
			get {
				// TODO: 
				return false;
			}
		}

		/// <summary>
		/// Returns true if this result set contains 1 column and 1 row and the name 
		/// of the column is <c>result</c>.
		/// </summary>
		/// <remarks>
		/// This indicates the result set is a DDL command ( <c>UPDATE</c>, <c>INSERT</c>, 
		/// <c>CREATE</c>, <c>ALTER</c>, etc ).
		/// <para>
		/// <b>NOTE:</b> This is a minor hack because there is no real indication that this 
		/// is a DML statement. Theoretically a SQL command could be constructed that meets a
		/// ll these requirements and is processed incorrectly.
		/// </para>
		/// </remarks>
		public bool IsUpdate {
			get {
				// Must have 1 col and 1 row and the title of the column must be
				// 'result' aliased.
				return (ColumnCount == 1 && RowCount == 1 &&
				        columns[0].Name.Equals("@aresult"));
			}
		}

		/// <summary>
		/// Ensures that the row index pointed to by 'real_index' is actually 
		/// loaded into the 'result_block'.
		/// </summary>
		/// <remarks>
		/// If not, we send a request to the database to get it.
		/// </remarks>
		private void EnsureIndexLoaded() {
			if (realIndex == -1)
				throw new DatabaseException("Row index out of bounds.");

			// Offset into our block
			int rowOffset = realIndex - blockTopRow;
			if (rowOffset >= blockRowCount) {
				// Need to download the next block from the server.
				Download(realIndex, fetchSize);
				// Set up the index into the downloaded block.
				rowOffset = realIndex - blockTopRow;
				realIndexOffset = rowOffset*ColumnCount;
			} else if (rowOffset < 0) {
				int fsDif = System.Math.Min(fetchSize, 8);
				// Need to download the next block from the server.
				Download(realIndex - fetchSize + fsDif, fetchSize);
				// Set up the index into the downloaded block.
				rowOffset = realIndex - blockTopRow;
				realIndexOffset = rowOffset*ColumnCount;
			}
		}

		/// <summary>
        /// Searches for the index of the column with the given name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns>
        /// Returns a zero-based index representing the position of the column 
        /// with the given name among those in the result.
        /// </returns>
        /// <exception cref="DatabaseException">
        /// If no column with the given name was found within the result.
        /// </exception>
		public int FindColumnIndex(string name) {
			// For speed, we keep column name -> column index mapping in the hashtable.
			// This makes column reference by string faster.
			if (columnHash == null) {
				var comparer = connection.Settings.IgnoreIdentifiersCase ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
				columnHash = new Dictionary<string, int>(comparer);
			}

	        int index;
			if (!columnHash.TryGetValue(name, out index)) {
				int colCount = ColumnCount;
				// First construct an unquoted list of all column names
				String[] cols = new String[colCount];
				for (int i = 0; i < colCount; ++i) {
					String colName = columns[i].Name;
					if (colName.StartsWith("\"")) {
						colName = colName.Substring(1, colName.Length - 2);
					}
					// Strip any codes from the name
					if (colName.StartsWith("@")) {
						colName = colName.Substring(2);
					}

					cols[i] = colName;
				}

				for (int i = 0; i < colCount; ++i) {
					String colName = cols[i];
					if (colName.Equals(name)) {
						columnHash[name] = i ;
						return i;
					}
				}

				// If not found then search for column name ending,
				string pointName = "." + name;
				for (int i = 0; i < colCount; ++i) {
					String colName = cols[i];
					if (colName.EndsWith(pointName)) {
						columnHash[name] = i;
						return i;
					}
				}

				throw new DatabaseException("Couldn't find column with name: " + name);
			}
			
			return index;
		}

		public object GetRuntimeValue(int ordinal) {
			var value = GetRawColumn(ordinal);

			if (value == null ||
				value == DBNull.Value)
				return DBNull.Value;

			var destType = GetColumn(ordinal).RuntimeType;

			if (value is BigNumber) {
				var number = (BigNumber) value;
				if (destType == typeof (byte))
					return number.ToByte();
				if (destType == typeof (short))
					return number.ToInt16();
				if (destType == typeof (int))
					return number.ToInt32();
				if (destType == typeof (long))
					return number.ToInt64();
				if (destType == typeof (float))
					return number.ToSingle();
				if (destType == typeof (double))
					return number.ToDouble();
				if (destType == typeof(decimal))
					throw new NotSupportedException();

				// TODO: throw an exception?
			}

			if (value is StringObject)
				return value.ToString();

			if (value is ByteLongObject) {
				var blob = (ByteLongObject) value;
				return blob.ToArray();
			}

			return value;
		}

		public object GetRawColumn(int column) {
			// ASSERTION -
			// Is the given column in bounds?
	        if (column < 0 || column >= ColumnCount)
		        throw new IndexOutOfRangeException("Column index out of bounds: 1 > " + column + " > " + ColumnCount);

	        // Ensure the current indexed row is fetched from the server.
			EnsureIndexLoaded();

			// Return the raw cell object.
			object ob = resultBlock.GetRow(realIndexOffset)[column];

			// Null check of the returned object,
			if (ob != null) {
				// If this is an object then deserialize it,
				// ISSUE: Cache deserialized objects?
				if (GetColumn(column).SQLType == SqlType.Object) {
					ob = ObjectTranslator.Deserialize((ByteLongObject)ob);
				}
				return ob;
			}
			return null;
		}

		/// <summary>
		/// Converts the current result into an integer, in case of a
		/// scalar result.
		/// </summary>
		/// <remarks>
		/// This is only valid if the result set has a single column and a single 
		/// row of type <see cref="BigNumber"/>.
		/// </remarks>
		/// <returns>
		/// Returns the integer value of the result.
		/// </returns>
		public int AffectedRows {
			get {
				if (!IsUpdate)
					throw new DatabaseException("Unable to format command result as an update value.");

				object ob = resultBlock.GetRow(0)[0];
				if (ob is BigNumber)
					return ((BigNumber) ob).ToInt32();

				return 0;
			}
		}

		public void Setup(int id, ColumnDescription[] columnList, int totalRowCount) {
			ResultId = id;
			columns = columnList;
			resultRowCount = totalRowCount;
			blockTopRow = -1;
			resultBlock = null;

			realIndex = -1;
			fetchSize = connection.Settings.FetchSize;
			Closed = false;
		}

		public void SetFetchSize(int rows) {
		    fetchSize = rows > 0 ? System.Math.Min(rows, MaxFetchSize) : DefaultFetchSize;
		}

		public void SetMaxRowCount(int rowCount) {
		    maxRowCount = rowCount == 0 ? Int32.MaxValue : rowCount;
		}

		public void DownloadAndClose() {
			Download(0, resultRowCount);
			connection.DisposeResult(ResultId);
			Closed = true;
		}

		public void Download(int rowIndex, int rowCount) {
			// If row_count is 0 then we don't need to do anything.
			if (rowCount == 0)
				return;

			if (rowIndex + rowCount < 0)
				throw new DatabaseException("ResultSet row index is before the start of the set.");

			if (rowIndex < 0) {
				rowIndex = 0;
				rowCount = rowCount + rowIndex;
			}

			if (rowIndex >= RowCount)
				throw new DatabaseException("ResultSet row index is after the end of the set.");
			if (rowIndex + rowCount > RowCount)
				rowCount = RowCount - rowIndex;

			if (ResultId == -1)
				throw new DatabaseException("result_id == -1.  No result to get from.");

			try {

				// Request the result via the RowCache.  If the information is not found
				// in the row cache then the request is forwarded onto the database.
				resultBlock = connection.RowCache.GetResultPart(ResultId, rowIndex, rowCount, ColumnCount, RowCount);

				// Set the row that's at the top
				blockTopRow = rowIndex;
				// Set the number of rows in the block.
				blockRowCount = rowCount;
			} catch (IOException e) {
				throw new DatabaseException("IO Error: " + e.Message);
			}
		}

		public ColumnDescription GetColumn(int offset) {
			return columns[offset];
		}

		private void RealIndexUpdate() {
			// Set up the index into the downloaded block.
			int rowOffset = realIndex - blockTopRow;
			realIndexOffset = rowOffset * ColumnCount;
		}

		public bool First() {
			realIndex = 0;
			RealIndexUpdate();
			return realIndex < RowCount;
		}

		public bool Next() {
			int rowCount = RowCount;
			if (realIndex < rowCount) {
				++realIndex;
				if (realIndex < rowCount) {
					RealIndexUpdate();
				}
			}
			return (realIndex < rowCount);
		}

		public void Close() {
			if (ResultId != -1) {
				if (!Closed) {
					// Request to close the current result set
					connection.DisposeResult(ResultId);
					Closed = true;
				}

				ResultId = -1;
				realIndex = Int32.MaxValue;

				// Clear the column name -> index mapping,
				if (columnHash != null) {
					columnHash.Clear();
				}
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				try {
					Close();
				} catch (DatabaseException) {
					// Ignore
					// We ignore exceptions because handling cases where the server
					// connection has broken for many ResultSets would be annoying.
				}

				connection = null;
				columns = null;
				resultBlock = null;
			}
		}
	}
}
