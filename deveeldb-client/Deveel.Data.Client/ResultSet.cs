using System;
using System.Collections;
using System.IO;

namespace Deveel.Data.Client {
	internal class ResultSet : IDisposable {
		public ResultSet(DeveelDbConnection connection, QueryResponse response) {
			this.connection = connection;
			this.response = response;
			resultBlock = new ArrayList();
		}

		~ResultSet() {
			Dispose(false);
		}

		private readonly DeveelDbConnection connection;
		private IList resultBlock;
		private QueryResponse response;
		private Hashtable columnsMap;
		private bool closedOnServer;

		private bool closed;
		private int realIndex = -1;
		private int fetchSize;
		private int maxRowCount;
		private int blockTopRow;
		private int blockRowCount;
		private int realIndexOffset = -1;


		public int FetchSize {
			get { return fetchSize; }
			set { fetchSize = value > 0 ? System.Math.Min(value, DeveelDbCommand.MaximumFetchSize) : DeveelDbCommand.DefaultFetchSize; }
		}

		public int MaxRowCount {
			get { return maxRowCount; }
			set { maxRowCount = value == 0 ? Int32.MaxValue : value; }
		}

		public bool HasLargeObjects {
			get {
				for (int i = 0; i < response.ColumnCount; ++i) {
					ColumnInfo col = response.GetColumn(i);
					if (col.Type == DeveelDbType.LOB)
						return true;
				}
				return false;
			}
		}

		public int ColumnCount {
			get { return response.ColumnCount; }
		}

		public int RowCount {
			get { return System.Math.Min(response.RowCount, maxRowCount); }
		}

		public bool IsUpdate {
			get { return ColumnCount == 1 && RowCount == 1 && GetColumnInfo(0).Name.Equals("@aresult"); }
		}

		public int ResultId {
			get { return response.ResultId; }
		}

		public bool IsClosed {
			get { return closed; }
		}

		private void RealIndexUpdate() {
			int row_offset = realIndex - blockTopRow;
			realIndexOffset = row_offset * ColumnCount;
		}

		private void EnsureIndexLoaded() {
			if (realIndex == -1)
				throw new DeveelDbException("Row index out of bounds.");

			// Offset into our block
			int rowOffset = realIndex - blockTopRow;
			if (rowOffset >= blockRowCount) {
				// Need to download the next block from the server.
				UpdateResultPart(realIndex, fetchSize);
				// Set up the index into the downloaded block.
				rowOffset = realIndex - blockTopRow;
				realIndexOffset = rowOffset * ColumnCount;
			} else if (rowOffset < 0) {
				int fs_dif = System.Math.Min(fetchSize, 8);
				// Need to download the next block from the server.
				UpdateResultPart(realIndex - fetchSize + fs_dif, fetchSize);
				// Set up the index into the downloaded block.
				rowOffset = realIndex - blockTopRow;
				realIndexOffset = rowOffset * ColumnCount;
			}
		}

		public ColumnInfo GetColumnInfo(int index) {
			return response.GetColumn(index);
		}

		public int ToInt32() {
			if (!IsUpdate)
				throw new DeveelDbException("Unable to format command result as an update value.");

			object ob = resultBlock[0];
			if (ob is DeveelDbNumber)
				return ((DeveelDbNumber) ob).ToInt32();

			return 0;
		}

		public bool Next() {
			int row_count = RowCount;
			if (realIndex < row_count) {
				++realIndex;
				if (realIndex < row_count)
					RealIndexUpdate();
			}
			return (realIndex < row_count);
		}

		public bool First() {
			realIndex = 0;
			RealIndexUpdate();
			return realIndex < RowCount;
		}

		public void Close() {
			if (response.ResultId != -1) {
				if (!closedOnServer) {
					connection.Driver.DisposeResult(response.ResultId);
					closedOnServer = true;
				}

				response = null;
				realIndex = Int32.MaxValue;

				if (columnsMap != null)
					columnsMap.Clear();
			}

			closed = true;
		}

		public void UpdateResultPart(int row_index, int row_count) {
			if (row_count == 0)
				return;

			if (row_index + row_count < 0)
				throw new DeveelDbException("Row index is before the start of the set.");

			if (row_index < 0) {
				row_index = 0;
				row_count = row_count + row_index;
			}

			if (row_index >= RowCount)
				throw new DeveelDbException("Row index is after the end of the set.");

			if (row_index + row_count > RowCount)
				row_count = RowCount - row_index;

			try {
				// Request the result via the RowCache.  If the information is not found
				// in the row cache then the request is forwarded onto the database.
				resultBlock = connection.RowCache.GetResultPart(resultBlock, response.ResultId, row_index, row_count, ColumnCount, RowCount);

				// Set the row that's at the top
				blockTopRow = row_index;
				// Set the number of rows in the block.
				blockRowCount = row_count;
			} catch (IOException e) {
				throw new DeveelDbException("IO Error: " + e.Message);
			}
		}

		public void DownloadResult() {
			// After this call, 'result_block' will contain the whole result set.
			UpdateResultPart(0, RowCount);
			// Request to close the current result set on the server.
			connection.Driver.DisposeResult(response.ResultId);
			closedOnServer = true;
		}

		public object GetRawColumn(int column) {
			if (column < 0 || column >= ColumnCount)
				throw new IndexOutOfRangeException("Column index out of bounds: 1 > " + column + " > " + ColumnCount);

			// Ensure the current indexed row is fetched from the server.
			EnsureIndexLoaded();
			// Return the raw cell object.
			return resultBlock[realIndexOffset + column];
		}

		public object GetRawColumn(string name) {
			return GetRawColumn(FindColumnIndex(name));
		}

		public int FindColumnIndex(string name) {
			if (columnsMap == null)
				columnsMap = new Hashtable();

			bool ignoreCase = connection.Settings.IgnoreIdentifiersCase;
			if (ignoreCase)
				name = name.ToUpper();

			object index = columnsMap[name];
			if (index == null) {
				int colCount = ColumnCount;
				// First construct an unquoted list of all column names
				string[] cols = new string[colCount];
				for (int i = 0; i < colCount; ++i) {
					string colName = response.GetColumn(i).Name;
					if (colName.StartsWith("\""))
						colName = colName.Substring(1, colName.Length - 2);

					// Strip any codes from the name
					if (colName.StartsWith("@"))
						colName = colName.Substring(2);
					if (ignoreCase)
						colName = colName.ToUpper();

					cols[i] = colName;
				}

				for (int i = 0; i < colCount; ++i) {
					string colName = cols[i];
					if (colName.Equals(name)) {
						columnsMap[name] = i;
						index = i;
						break;
					}
				}

				if (index == null) {
					// If not found then search for column name ending,
					string point_name = "." + name;
					for (int i = 0; i < colCount; ++i) {
						string colName = cols[i];
						if (colName.EndsWith(point_name)) {
							columnsMap[name] = i;
							index = i;
							break;
						}
					}
				}

				if (index == null)
					throw new DeveelDbException("Couldn't find column with name: " + name);
			}

			return (int) index;
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				Close();

				response = null;
				resultBlock = null;
			}
		}

		#region Implementation of IDisposable

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		#endregion
	}
}