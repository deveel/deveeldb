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
using System.Data;
using System.IO;

using Deveel.Data.Client;

namespace Deveel.Data.Protocol {
    /// <summary>
    /// An object which represents the result of a command on the server.
    /// </summary>
    /// <remarks>
    /// This class is not designed to be multi-thread safe. A result-set should 
    /// not be accessed by concurrent threads.
    /// </remarks>
	sealed class ResultSet {
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
		private static int unique_id_key = 1;


        /// <summary>
        /// A unique int that refers to this result set.
        /// </summary>
		private int unique_id;

        /// <summary>
        /// The <see cref="DeveelDbConnection"/> that this result set is in.
        /// </summary>
		internal DeveelDbConnection connection;

        /// <summary>
        /// The current result_id for the information in the current result set.
        /// </summary>
		internal int result_id;

        /// <summary>
        /// The array of <see cref="ColumnDescription"/> that describes each column 
        /// in the result set.
        /// </summary>
		private ColumnDescription[] col_list;

        /// <summary>
        /// The length of time it took to execute this command in ms.
        /// </summary>
		private int query_time_ms;

        /// <summary>
        /// The number of rows in the result set.
        /// </summary>
		private int result_row_count;

        /// <summary>
        /// The maximum row count as set in the <see cref="DeveelDbCommand"/> by the 
        /// <see cref="SetMaxRowCount"/> method or 0 if the max row count is 
        /// not important.
        /// </summary>
		private int max_row_count = Int32.MaxValue;

        /// <summary>
        /// The row number of the first row of the 'result_block'
        /// </summary>
		private int block_top_row;

        /// <summary>
        /// The number of rows in 'result_block'
        /// </summary>
		private int block_row_count;

        /// <summary>
        /// The number of rows to fetch each time we need to get rows from 
        /// the database.
        /// </summary>
		private int fetch_size;

        /// <summary>
        /// The <see cref="ArrayList"/> that contains the objects downloaded into 
        /// this result set.
        /// </summary>
        /// <remarks>
        /// It only contains the objects from the last block of rows downloaded.
        /// </remarks>
		private ArrayList result_block;

        /// <summary>
        /// The real index of the result set we are currently at.
        /// </summary>
		private int real_index = Int32.MaxValue;

        /// <summary>
        /// The offset into 'result_block' where 'real_index' is.
        /// </summary>
        /// <remarks>
        /// This is set up by <see cref="EnsureIndexLoaded"/>.
        /// </remarks>
		private int real_index_offset = -1;

        /// <summary>
        /// A <see cref="Hashtable"/> that acts as a cache for column 
        /// name/column number look ups.
        /// </summary>
		private Hashtable column_hash;

        /// <summary>
        /// Set to true if the result set is closed on the server.
        /// </summary>
		internal bool closed_on_server;


		internal ResultSet(DeveelDbConnection connection) {
			this.connection = connection;
			unique_id = unique_id_key++;
			result_id = -1;
			result_block = new ArrayList();
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
        /// Called by the <see cref="ConnectionThread"/> when we have received the 
        /// initial bag of the result set.
        /// </summary>
        /// <param name="result_id"></param>
        /// <param name="col_list"></param>
        /// <param name="total_row_count"></param>
        /// <remarks>
        /// This contains information about the columns in the result, the number of rows 
        /// in the entire set, etc.  This sets up the result set to handle the result.
        /// </remarks>
		internal void ConnSetup(int result_id, ColumnDescription[] col_list, int total_row_count) {
			this.result_id = result_id;
			this.col_list = col_list;
			this.result_row_count = total_row_count;
			block_top_row = -1;
			result_block.Clear();

			real_index = -1;
			fetch_size = DefaultFetchSize;
			closed_on_server = false;
		}

        /// <summary>
        /// Sets the length of time in milliseconds (server-side) it took to execute 
        /// this command.
        /// </summary>
        /// <param name="time_ms"></param>
        /// <remarks>
        /// Useful as feedback for the server-side optimisation systems.
        /// <para>
        /// An int can <i>only</i> contain 35 weeks worth of milliseconds. So if a command 
        /// takes longer than that this number will overflow.
        /// </para>
        /// </remarks>
		internal void SetQueryTime(int time_ms) {
			query_time_ms = time_ms;
		}

        /// <summary>
        /// Sets the maximum number of rows that this result-set will return or 
        /// 0 if the max number of rows is not important.
        /// </summary>
        /// <remarks>
        /// This is set by <see cref="DeveelDbCommand"/> when a command is evaluated.
        /// </remarks>
		internal void SetMaxRowCount(int rowCount) {
		    max_row_count = rowCount == 0 ? Int32.MaxValue : rowCount;
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
			for (int i = 0; i < col_list.Length; ++i) {
				ColumnDescription col = col_list[i];
				SqlType sql_type = col.SQLType;
				if (sql_type == SqlType.Binary ||
					sql_type == SqlType.VarBinary ||
					sql_type == SqlType.LongVarBinary ||
					sql_type == SqlType.Blob ||
					sql_type == SqlType.Char ||
					sql_type == SqlType.VarChar ||
					sql_type == SqlType.LongVarChar ||
					sql_type == SqlType.Clob) {
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
			connection.DisposeResult(result_id);
			closed_on_server = true;
		}

        /// <summary>
        /// Asks the server for more information about this result set to write 
        /// into the 'result_block'.
        /// </summary>
        /// <param name="row_index">The top row index from the block of the result 
        /// set to download.</param>
        /// <param name="row_count">The maximum number of rows to download (may be 
        /// less if no more are available).</param>
        /// <remarks>
        /// This should be called when we need to request more information from 
        /// the server.
        /// </remarks>
		internal void UpdateResultPart(int row_index, int row_count) {
			// If row_count is 0 then we don't need to do anything.
			if (row_count == 0)
				return;

			if (row_index + row_count < 0)
				throw new DataException("ResultSet row index is before the start of the set.");

			if (row_index < 0) {
				row_index = 0;
				row_count = row_count + row_index;
			}

			if (row_index >= RowCount)
				throw new DataException("ResultSet row index is after the end of the set.");
			if (row_index + row_count > RowCount)
				row_count = RowCount - row_index;

			if (result_id == -1)
				throw new DataException("result_id == -1.  No result to get from.");

			try {

				// Request the result via the RowCache.  If the information is not found
				// in the row cache then the request is forwarded onto the database.
				result_block = connection.RowCache.GetResultPart(result_block,
							connection, result_id, row_index, row_count,
							ColumnCount, RowCount);

				// Set the row that's at the top
				block_top_row = row_index;
				// Set the number of rows in the block.
				block_row_count = row_count;
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
				if (!closed_on_server) {
					// Request to close the current result set
					connection.DisposeResult(result_id);
					closed_on_server = true;
				}
				result_id = -1;
				real_index = Int32.MaxValue;
				// Clear the column name -> index mapping,
				if (column_hash != null) {
					column_hash.Clear();
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
	    internal int ResultId {
	        get { return result_id; }
	    }

	    /// <summary>
        /// The total number of rows in the result set.
        /// </summary>
	    internal int RowCount {
	        get {
	            // The row count is whatever is the least between max_row_count (the
	            // maximum the user has set) and result_row_count (the actual number of
	            // rows in the result.
	            return System.Math.Min(result_row_count, max_row_count);
	        }
	    }

	    /// <summary>
        /// The column count of columns in the result set.
        /// </summary>
	    internal int ColumnCount {
	        get { return col_list.Length; }
	    }

	    /// <summary>
        /// Returns the ColumnDescription of the given column
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
		internal ColumnDescription GetColumn(int column) {
			return col_list[column];
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
			if (IsUpdate) {
				Object ob = result_block[0];
				if (ob is BigNumber) {
					return ((BigNumber)ob).ToInt32();
				} else {
					return 0;
				}
			}
			throw new DataException("Unable to format command result as an update value.");
		}


        /// <summary>
        /// Disposes of all resources associated with this result set.
        /// </summary>
        /// <remarks>
        /// This could either be called from the <see cref="DeveelDbCommand.Dispose"/> method. Calls 
        /// to this object are undefined after this method has finished.
        /// </remarks>
		internal void Dispose() {
			try {
				Close();
			} catch (DataException) {
				// Ignore
				// We ignore exceptions because handling cases where the server
				// connection has broken for many ResultSets would be annoying.
			}

			connection = null;
			col_list = null;
			result_block = null;
		}

        /// <summary>
        /// Ensures that the row index pointed to by 'real_index' is actually 
        /// loaded into the 'result_block'.
        /// </summary>
        /// <remarks>
        /// If not, we send a request to the database to get it.
        /// </remarks>
		void EnsureIndexLoaded() {
			if (real_index == -1) {
				throw new DataException("Row index out of bounds.");
			}

			// Offset into our block
			int row_offset = real_index - block_top_row;
			if (row_offset >= block_row_count) {
				// Need to download the next block from the server.
				UpdateResultPart(real_index, fetch_size);
				// Set up the index into the downloaded block.
				row_offset = real_index - block_top_row;
				real_index_offset = row_offset * ColumnCount;
			} else if (row_offset < 0) {
				int fs_dif = System.Math.Min(fetch_size, 8);
				// Need to download the next block from the server.
				UpdateResultPart(real_index - fetch_size + fs_dif, fetch_size);
				// Set up the index into the downloaded block.
				row_offset = real_index - block_top_row;
				real_index_offset = row_offset * ColumnCount;
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
		internal int FindColumnIndex(String name) {
			// For speed, we keep column name -> column index mapping in the hashtable.
			// This makes column reference by string faster.
			if (column_hash == null) {
				column_hash = new Hashtable();
			}

			bool case_insensitive = connection.IsCaseInsensitiveIdentifiers;
			if (case_insensitive) {
				name = name.ToUpper();
			}

			if (!column_hash.ContainsKey(name)) {
				int col_count = ColumnCount;
				// First construct an unquoted list of all column names
				String[] cols = new String[col_count];
				for (int i = 0; i < col_count; ++i) {
					String col_name = col_list[i].Name;
					if (col_name.StartsWith("\"")) {
						col_name = col_name.Substring(1, col_name.Length - 2);
					}
					// Strip any codes from the name
					if (col_name.StartsWith("@")) {
						col_name = col_name.Substring(2);
					}
					if (case_insensitive) {
						col_name = col_name.ToUpper();
					}
					cols[i] = col_name;
				}

				for (int i = 0; i < col_count; ++i) {
					String col_name = cols[i];
					if (col_name.Equals(name)) {
						column_hash[name] = i ;
						return i;
					}
				}

				// If not found then search for column name ending,
				string point_name = "." + name;
				for (int i = 0; i < col_count; ++i) {
					String col_name = cols[i];
					if (col_name.EndsWith(point_name)) {
						column_hash[name] = i;
						return i;
					}
				}

				throw new DataException("Couldn't find column with name: " + name);
			}
			
			return (int)column_hash[name];
		}

        /// <summary>
        /// Returns the column Object of the current index.
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
		internal Object GetRawColumn(int column) {
			// ASSERTION -
			// Is the given column in bounds?
			if (column < 0 || column >= ColumnCount) {
				throw new IndexOutOfRangeException("Column index out of bounds: 1 > " + column + " > " + ColumnCount);
			}
			// Ensure the current indexed row is fetched from the server.
			EnsureIndexLoaded();
			// Return the raw cell object.
			Object ob = result_block[real_index_offset + column];
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
			int row_offset = real_index - block_top_row;
			real_index_offset = row_offset * ColumnCount;
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
            get { return query_time_ms; }
        }

        public bool Next() {
			int row_count = RowCount;
			if (real_index < row_count) {
				++real_index;
				if (real_index < row_count) {
					RealIndexUpdate();
				}
			}
			return (real_index < row_count);
		}

		public void Close() {
			CloseCurrentResult();
		}

		public void SetFetchSize(int rows) {
		    fetch_size = rows > 0 ? System.Math.Min(rows, MaximumFetchSize) : DefaultFetchSize;
		}

    	public bool First() {
			real_index = 0;
			RealIndexUpdate();
			return real_index < RowCount;
		}

    	~ResultSet() {
            Dispose();
        }
	}
}