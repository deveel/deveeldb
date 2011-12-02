// 
//  DeveelDbCommand.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;
using System.Data.Common;
using System.IO;

namespace Deveel.Data.Client {
	public sealed class DeveelDbCommand : DbCommand, ICloneable {
		public DeveelDbCommand() {
			parameters = new DeveelDbParameterCollection(this);
		}

		public DeveelDbCommand(string commandText)
			: this() {
			CommandText = commandText;
		}

		public DeveelDbCommand(string commandText, DeveelDbConnection connection)
			: this(commandText) {
			Connection = connection;
		}

		public DeveelDbCommand(string commandText, DeveelDbConnection connection, DeveelDbTransaction transaction)
			: this(commandText, connection) {
			this.transaction = transaction;
		}

		private bool designTimeVisible;
		private DeveelDbConnection connection;
		private DeveelDbTransaction transaction;
		private int commandTimeout;
		private bool timeoutWasSet;
		private string commandText;
		private readonly DeveelDbParameterCollection parameters;
		private int fetchSize = DefaultFetchSize;
		private int maxRowCount;
		private DeveelDbDataReader reader;

		public const int MaximumFetchSize = 512;
		public const int DefaultFetchSize = 32;

		private ResultSet result;

		public override void Prepare() {
			if (connection == null)
				throw new InvalidOperationException();
			if (connection.State != ConnectionState.Open)
				throw new InvalidOperationException();

			//TODO:
		}

		public override string CommandText {
			get { return commandText; }
			set { commandText = value; }
		}

		public int FetchSize {
			get { return fetchSize; }
			set {
				if (value > MaximumFetchSize)
					throw new ArgumentOutOfRangeException();
				fetchSize = value;
			}
		}

		public int MaxRowCount {
			get { return maxRowCount; }
			set { maxRowCount = value; }
		}

		public override int CommandTimeout {
			get {
				if (timeoutWasSet)
					return commandTimeout;
				if (connection != null)
					return connection.Settings.QueryTimeout;
				return -1;
			}
			set {
				if (value < 0) {
					timeoutWasSet = false;
					commandTimeout = -1;
				} else {
					commandTimeout = value;
					timeoutWasSet = true;
				}
			}
		}

		public override CommandType CommandType {
			get { return CommandType.Text; }
			set {
				if (value != CommandType.Text)
					throw new NotSupportedException();	// yet...
			}
		}

		public override UpdateRowSource UpdatedRowSource {
			get { return UpdateRowSource.None; }
			set {
				if (value != UpdateRowSource.None)
					throw new NotSupportedException();	// yet...
			}
		}

		protected override DbConnection DbConnection {
			get { return Connection; }
			set { Connection = (DeveelDbConnection) value; }
		}

		public new DeveelDbConnection Connection {
			get { return connection; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				if (connection != value)
					Transaction = null;

				connection = value;
				transaction = connection.currentTransaction;
			}
		}

		protected override DbParameterCollection DbParameterCollection {
			get { return Parameters; }
		}

		public new DeveelDbParameterCollection Parameters {
			get { return parameters; }
		}

		protected override DbTransaction DbTransaction {
			get { return Transaction; }
			set { Transaction = (DeveelDbTransaction) value; }
		}

		public new DeveelDbTransaction Transaction {
			get { return transaction; }
			set {
				if (value == null && transaction != null)
					transaction = null;
				else if (transaction != null &&
					(value != null && value.Id != transaction.Id))
					throw new ArgumentException();

				transaction = value;
			}
		}

		public override bool DesignTimeVisible {
			get { return designTimeVisible; }
			set { designTimeVisible = value; }
		}

		private ResultSet ExecuteQuery() {
			if (connection == null)
				throw new InvalidOperationException("The connection was not set.");
			if (connection.State == ConnectionState.Closed)
				throw new InvalidOperationException("The connection is closed.");

			if (commandText == null || commandText.Length == 0)
				throw new InvalidOperationException("The command text was not set.");

			if (result != null) {
				result.Dispose();
				result = null;
			}

			UploadLargeObjects();
			QueryResponse response = connection.Driver.ExecuteQuery(this);
			result = new ResultSet(connection, response);

			result.FetchSize = fetchSize;
			result.MaxRowCount = maxRowCount;

			// Does the result set contain large objects?  We can't cache a
			// result that contains binary data.

			// If the result row count < 40 then download and store locally in the
			// result set and dispose the resources on the server.
			if (!result.HasLargeObjects && result.RowCount < 40) {
				result.DownloadResult();
			} else {
				result.UpdateResultPart(0, System.Math.Min(10, result.RowCount));
			}

			return result;
		}

		private void UploadLargeObjects() {
			try {
				for (int i = 0; i < parameters.Count; ++i) {
					DeveelDbParameter parameter = parameters[i];
					if (!parameter.IsNull && parameter.Value is LargeObjectRef) {
						// Buffer size is fixed to 64 KB
						const int BUF_SIZE = 64 * 1024;

						LargeObjectRef objectRef = (LargeObjectRef)parameter.Value;
						long offset = 0;
						ReferenceType type = objectRef.Type;
						long totalLen = objectRef.Size;
						long id = objectRef.Id;
						byte[] buf = new byte[BUF_SIZE];

						Stream stream = connection.Driver.GetLargeObject(objectRef);
						if (stream == null)
							throw new Exception("Object Stream is not available.");

						stream.Seek(0, SeekOrigin.Begin);

						while (offset < totalLen) {
							// Fill the buffer
							int index = 0;
							int block_read = (int)System.Math.Min(BUF_SIZE, (totalLen - offset));
							int to_read = block_read;
							while (to_read > 0) {
								int count = stream.Read(buf, index, to_read);
								if (count == -1)
									throw new IOException("Premature end of stream.");

								index += count;
								to_read -= count;
							}

							connection.Driver.PushLargeObjectPart(type, id, totalLen, buf, offset, block_read);
							// Increment the offset and upload the next part of the object.
							offset += block_read;
						}

						// Remove the streamable object once it has been written
						connection.Driver.RemoveLargeObject(objectRef);
					}
				}
			} catch (IOException e) {
				throw new DeveelDbException("IO Error pushing large object to server: " + e.Message);
			}
		}

		public override void Cancel() {
			if (reader != null)
				reader.Close();

			if (result != null)
				connection.Driver.DisposeResult(result.ResultId);

			connection.SetState(ConnectionState.Open, true);
		}

		protected override DbParameter CreateDbParameter() {
			return CreateParameter();
		}

		public new DeveelDbParameter CreateParameter() {
			return new DeveelDbParameter();
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			if (behavior != CommandBehavior.Default)
				throw new NotSupportedException();	// yet...

			return ExecuteReader();
		}

		public new DeveelDbDataReader ExecuteReader() {
			if (reader != null)
				throw new InvalidOperationException("A reader is already opened for this command.");

			if (connection.State == ConnectionState.Fetching)
				throw new InvalidOperationException("The connection is already busy fetching data.");

			connection.SetState(ConnectionState.Fetching, true);

			try {
				ExecuteQuery();

				reader = new DeveelDbDataReader(this, result);
				return reader;
			} catch(DeveelDbException) {
				connection.SetState(ConnectionState.Open, true);
				throw;
			} catch(Exception e) {
				connection.SetState(ConnectionState.Open, true);
				throw new DeveelDbException("Error while fetching from database." + e.Message);
			}
		}

		internal void OnReaderClosed() {
			connection.SetState(ConnectionState.Open, true);
			reader = null;
		}

		public override int ExecuteNonQuery() {
			connection.SetState(ConnectionState.Executing, true);

			try {
				ExecuteQuery();

				int affected = 0;
				if (result.IsUpdate)
					affected = result.ToInt32();

				return affected;
			} catch(Exception) {
				throw new DeveelDbException();
			} finally {
				connection.SetState(ConnectionState.Open, true);
			}
		}

		public override object ExecuteScalar() {
			connection.SetState(ConnectionState.Fetching, true);

			try {
				ExecuteQuery();

				if (result.RowCount > 1)
					throw new DeveelDbException();
				if (result.ColumnCount > 1)
					throw new DeveelDbException();

				if (!result.First())
					return null;

				return result.GetRawColumn(0);
			} catch(Exception) {
				throw new DeveelDbException();
			} finally {
				connection.SetState(ConnectionState.Open, true);
			}
		}

		#region Implementation of ICloneable

		object ICloneable.Clone() {
			DeveelDbCommand command = new DeveelDbCommand((string)commandText.Clone(), connection, transaction);
			command.CommandTimeout = CommandTimeout;
			foreach (DeveelDbParameter parameter in Parameters)
				command.Parameters.Add((DeveelDbParameter)(parameter as ICloneable).Clone());
			return command;
		}

		#endregion
	}
}