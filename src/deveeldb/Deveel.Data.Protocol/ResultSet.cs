// 
//  Copyright 2010  Deveel
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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;

using Deveel.Data.Client;
using Deveel.Data.Sql;

namespace Deveel.Data.Protocol {
    /// <summary>
    /// An object which represents the result of a command on the server.
    /// </summary>
    /// <remarks>
    /// This class is not designed to be multi-thread safe. A result-set should 
    /// not be accessed by concurrent threads.
    /// </remarks>
	sealed class ResultSet : IDisposable {
        /// <summary>
        /// The default fetch size.
        /// </summary>
		private const int DefaultFetchSize = 32;

        /// <summary>
        /// The maximum fetch size.
        /// </summary>
		private const int MaximumFetchSize = 512;

        /// <summary>
        /// The current unique id key.
        /// </summary>
		private static int uniqueIdKey = 1;


        /// <summary>
        /// A unique int that refers to this result set.
        /// </summary>
		private int uniqueId;

        /// <summary>
        /// The <see cref="DeveelDbConnection"/> that this result set is in.
        /// </summary>
		internal DeveelDbConnection connection;

	    /// <summary>
        /// The array of <see cref="ColumnDescription"/> that describes each column 
        /// in the result set.
        /// </summary>
		private ColumnDescription[] colList;

        /// <summary>
        /// The length of time it took to execute this command in ms.
        /// </summary>
		private int queryTimeMs;

        /// <summary>
        /// The number of rows in the result set.
        /// </summary>
		private int resultRowCount;

        /// <summary>
        /// The maximum row count as set in the <see cref="DeveelDbCommand"/> by the 
        /// <see cref="SetMaxRowCount"/> method or 0 if the max row count is 
        /// not important.
        /// </summary>
		private int maxRowCount = Int32.MaxValue;

        /// <summary>
        /// The row number of the first row of the 'result_block'
        /// </summary>
		private int blockTopRow;

        /// <summary>
        /// The number of rows in 'result_block'
        /// </summary>
		private int blockRowCount;

        /// <summary>
        /// The number of rows to fetch each time we need to get rows from 
        /// the database.
        /// </summary>
		private int fetchSize;

        /// <summary>
        /// The <see cref="ArrayList"/> that contains the objects downloaded into 
        /// this result set.
        /// </summary>
        /// <remarks>
        /// It only contains the objects from the last block of rows downloaded.
        /// </remarks>
		private List<object> resultBlock;

        /// <summary>
        /// The real index of the result set we are currently at.
        /// </summary>
		private int realIndex = Int32.MaxValue;

        /// <summary>
        /// The offset into 'result_block' where 'real_index' is.
        /// </summary>
        /// <remarks>
        /// This is set up by <see cref="EnsureIndexLoaded"/>.
        /// </remarks>
		private int realIndexOffset = -1;

        /// <summary>
        /// A <see cref="Hashtable"/> that acts as a cache for column 
        /// name/column number look ups.
        /// </summary>
		private Dictionary<string, int> columnHash;

        /// <summary>
        /// Set to true if the result set is closed on the server.
        /// </summary>
		internal bool ClosedOnServer;


		internal ResultSet(DeveelDbConnection connection) {
			this.connection = connection;
			uniqueId = uniqueIdKey++;
			ResultId = -1;
			resultBlock = new List<object>();
		}

        /// <summary>
        /// Returns true if verbose column names are enabled on the connection.
        /// </summary>
        /// <remarks>
        /// Returns false by default.
        /// </remarks>
	    internal bool VerboseColumnNames {
	        get { return connection.Settings.VerboseColumnNames; }
	    }

	    // ---------- Connection callback methods ----------
		// These methods are called back from the ConnectionThread running on the
		// connection.  These methods require some synchronization thought.

        /// <summary>
        /// Called when we have received the initial bag of the result set.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="columns"></param>
        /// <param name="totalRowCount"></param>
        /// <remarks>
        /// This contains information about the columns in the result, the number of rows 
        /// in the entire set, etc.  This sets up the result set to handle the result.
        /// </remarks>
		internal void ConnSetup(int id, ColumnDescription[] columns, int totalRowCount) {
			ResultId = id;
			colList = columns;
			resultRowCount = totalRowCount;
			blockTopRow = -1;
			resultBlock.Clear();

			realIndex = -1;
			fetchSize = DefaultFetchSize;
			ClosedOnServer = false;
		}

        /// <summary>
        /// Sets the length of time in milliseconds (server-side) it took to execute 
        /// this command.
        /// </summary>
        /// <param name="value"></param>
        /// <remarks>
        /// Useful as feedback for the server-side optimisation systems.
        /// <para>
        /// An int can <i>only</i> contain 35 weeks worth of milliseconds. So if a command 
        /// takes longer than that this number will overflow.
        /// </para>
        /// </remarks>
		internal void SetQueryTime(int value) {
			queryTimeMs = value;
		}

        /// <summary>
        /// Sets the maximum number of rows that this result-set will return or 
        /// 0 if the max number of rows is not important.
        /// </summary>
        /// <remarks>
        /// This is set by <see cref="DeveelDbCommand"/> when a command is evaluated.
        /// </remarks>
		internal void SetMaxRowCount(int rowCount) {
		    maxRowCount = rowCount == 0 ? Int32.MaxValue : rowCount;
		}

	    /// <summary>
        /// Verifies if the current result-set has large objects.
        /// </summary>
        /// <remarks>
        /// This looks at the ColumnDescription object to determine this.
        /// </remarks>
        /// <returns>
        /// Returns <b>true</b> if this result-set contains large objects.
        /// </returns>
		internal bool ContainsLargeObjects() {
			for (int i = 0; i < colList.Length; ++i) {
				ColumnDescription col = colList[i];
				SqlType sqlType = col.SQLType;
				if (sqlType == SqlType.Binary ||
					sqlType == SqlType.VarBinary ||
					sqlType == SqlType.LongVarBinary ||
					sqlType == SqlType.Blob ||
					sqlType == SqlType.Char ||
					sqlType == SqlType.VarChar ||
					sqlType == SqlType.LongVarChar ||
					sqlType == SqlType.Clob) {
					return true;
				}
			}
			return false;
		}

        /// <summary>
        /// Asks the server for all the rows in the result set and stores it locally 
        /// within this object.
        /// </summary>
        /// <remarks>
        /// It then disposes all resources associated with this result set on the server.
        /// </remarks>
		internal void StoreResultLocally() {
			// After this call, 'result_block' will contain the whole result set.
			UpdateResultPart(0, RowCount);
			// Request to close the current result set on the server.
			connection.DisposeResult(ResultId);
			ClosedOnServer = true;
		}

        /// <summary>
        /// Asks the server for more information about this result set to write 
        /// into the 'result_block'.
        /// </summary>
        /// <param name="rowIndex">The top row index from the block of the result 
        /// set to download.</param>
        /// <param name="rowCount">The maximum number of rows to download (may be 
        /// less if no more are available).</param>
        /// <remarks>
        /// This should be called when we need to request more information from 
        /// the server.
        /// </remarks>
		internal void UpdateResultPart(int rowIndex, int rowCount) {
			// If row_count is 0 then we don't need to do anything.
			if (rowCount == 0)
				return;

			if (rowIndex + rowCount < 0)
				throw new DataException("ResultSet row index is before the start of the set.");

			if (rowIndex < 0) {
				rowIndex = 0;
				rowCount = rowCount + rowIndex;
			}

			if (rowIndex >= RowCount)
				throw new DataException("ResultSet row index is after the end of the set.");
			if (rowIndex + rowCount > RowCount)
				rowCount = RowCount - rowIndex;

			if (ResultId == -1)
				throw new DataException("result_id == -1.  No result to get from.");

			try {

				// Request the result via the RowCache.  If the information is not found
				// in the row cache then the request is forwarded onto the database.
				resultBlock = connection.RowCache.GetResultPart(resultBlock, connection, ResultId, rowIndex, rowCount, ColumnCount, RowCount);

				// Set the row that's at the top
				blockTopRow = rowIndex;
				// Set the number of rows in the block.
				blockRowCount = rowCount;
			} catch (IOException e) {
				throw new DataException("IO Error: " + e.Message);
			}
		}

        /// <summary>
        /// Closes the current server side result for this result set ready 
        /// for a new one.
        /// </summary>
        /// <remarks>
        /// This should be called before we execute a command.  It sends a command 
        /// to the server to despose of any resources associated with the current 
        /// result_id.
        /// <para>
        /// It's perfectly safe to call this method even if we haven't downloaded 
        /// a result set from the server and you may also safely call it multiple
        /// times (it will only send one request to the server).
        /// </para>
        /// </remarks>
		internal void CloseCurrentResult() {
			if (ResultId != -1) {
				if (!ClosedOnServer) {
					// Request to close the current result set
					connection.DisposeResult(ResultId);
					ClosedOnServer = true;
				}

				ResultId = -1;
				realIndex = Int32.MaxValue;

				// Clear the column name -> index mapping,
				if (columnHash != null) {
					columnHash.Clear();
				}
			}
		}

	    /// <summary>
	    /// Returns the identificator that is used as a key to refer to the result set 
	    /// on the server that is the result of the command.
	    /// </summary>
	    /// <remarks>
	    /// An identificator of -1 means there is no server side result set associated 
	    /// with this object.
	    /// </remarks>
	    internal int ResultId { get; set; }

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
	        get { return colList.Length; }
	    }

	    /// <summary>
        /// Returns the ColumnDescription of the given column
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
		internal ColumnDescription GetColumn(int column) {
			return colList[column];
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
        internal bool IsUpdate {
            get {
                // Must have 1 col and 1 row and the title of the column must be
                // 'result' aliased.
                return (ColumnCount == 1 && RowCount == 1 &&
                        GetColumn(0).Name.Equals("@aresult"));
            }
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
		internal int ToInteger() {
	        if (!IsUpdate)
		        throw new DataException("Unable to format command result as an update value.");

	        object ob = resultBlock[0];
	        if (ob is BigNumber)
		        return ((BigNumber) ob).ToInt32();

	        return 0;
        }


        /// <summary>
        /// Disposes of all resources associated with this result set.
        /// </summary>
        /// <remarks>
        /// This could either be called from the <see cref="DeveelDbCommand.Dispose"/> method. Calls 
        /// to this object are undefined after this method has finished.
        /// </remarks>
		public void Dispose() {
			try {
				Close();
			} catch (DataException) {
				// Ignore
				// We ignore exceptions because handling cases where the server
				// connection has broken for many ResultSets would be annoying.
			}

			connection = null;
			colList = null;
			resultBlock = null;
		}

        /// <summary>
        /// Ensures that the row index pointed to by 'real_index' is actually 
        /// loaded into the 'result_block'.
        /// </summary>
        /// <remarks>
        /// If not, we send a request to the database to get it.
        /// </remarks>
		void EnsureIndexLoaded() {
			if (realIndex == -1) {
				throw new DataException("Row index out of bounds.");
			}

			// Offset into our block
			int rowOffset = realIndex - blockTopRow;
			if (rowOffset >= blockRowCount) {
				// Need to download the next block from the server.
				UpdateResultPart(realIndex, fetchSize);
				// Set up the index into the downloaded block.
				rowOffset = realIndex - blockTopRow;
				realIndexOffset = rowOffset * ColumnCount;
			} else if (rowOffset < 0) {
				int fsDif = System.Math.Min(fetchSize, 8);
				// Need to download the next block from the server.
				UpdateResultPart(realIndex - fetchSize + fsDif, fetchSize);
				// Set up the index into the downloaded block.
				rowOffset = realIndex - blockTopRow;
				realIndexOffset = rowOffset * ColumnCount;
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
        /// <exception cref="DataException">
        /// If no column with the given name was found within the result.
        /// </exception>
		internal int FindColumnIndex(string name) {
			// For speed, we keep column name -> column index mapping in the hashtable.
			// This makes column reference by string faster.
			if (columnHash == null) {
				var comparer = connection.IsCaseInsensitiveIdentifiers ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
				columnHash = new Dictionary<string, int>(comparer);
			}

	        int index;
			if (!columnHash.TryGetValue(name, out index)) {
				int colCount = ColumnCount;
				// First construct an unquoted list of all column names
				String[] cols = new String[colCount];
				for (int i = 0; i < colCount; ++i) {
					String colName = colList[i].Name;
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

				throw new DataException("Couldn't find column with name: " + name);
			}
			
			return index;
		}

        /// <summary>
        /// Returns the column Object of the current index.
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
		internal object GetRawColumn(int column) {
			// ASSERTION -
			// Is the given column in bounds?
	        if (column < 0 || column >= ColumnCount)
		        throw new IndexOutOfRangeException("Column index out of bounds: 1 > " + column + " > " + ColumnCount);

	        // Ensure the current indexed row is fetched from the server.
			EnsureIndexLoaded();

			// Return the raw cell object.
			object ob = resultBlock[realIndexOffset + column];

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
        /// Returns the column Object of the name of the current index.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
		internal Object GetRawColumn(String name) {
			return GetRawColumn(FindColumnIndex(name));
		}

        /// <summary>
        /// This should be called when the 'real_index' variable changes.
        /// </summary>
        /// <remarks>
        /// It updates internal variables.
        /// </remarks>
		private void RealIndexUpdate() {
			// Set up the index into the downloaded block.
			int rowOffset = realIndex - blockTopRow;
			realIndexOffset = rowOffset * ColumnCount;
			// Clear any warnings as in the spec.
			// clearWarnings();
		}


        /// <summary>
        /// The number of milliseconds it took the server to execute this command.
        /// </summary>
        /// <remarks>
        /// This is set after the call to <see cref="ConnSetup"/> so is available 
        /// as soon as the header information is received from the server.
        /// </remarks>
        public int QueryTimeMillis {
            get { return queryTimeMs; }
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
			CloseCurrentResult();
		}

		public void SetFetchSize(int rows) {
		    fetchSize = rows > 0 ? global::System.Math.Min(rows, MaximumFetchSize) : DefaultFetchSize;
		}

    	public bool First() {
			realIndex = 0;
			RealIndexUpdate();
			return realIndex < RowCount;
		}

    	~ResultSet() {
            Dispose();
        }
	}
}