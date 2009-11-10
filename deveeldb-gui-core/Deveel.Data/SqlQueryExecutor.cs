using System;
using System.Data;
using System.Data.Common;
using System.Text;

using Deveel.Data.Client;

namespace Deveel.Data {
	public sealed class SqlQueryExecutor {
		public SqlQueryExecutor(string connectionString) {
			this.connectionString = connectionString;
		}

		private readonly string connectionString;
		private bool busy;
		private SqlQueryBatch batch;
		private DbException exception;
		private readonly StringBuilder messages = new StringBuilder();

		public event QueryEventHandler QueryExecuted;

		public bool IsBusy {
			get { return busy; }
		}

		public string ErrorMessages {
			get { return messages.ToString(); }
		}

		public DbException LastError {
			get { return exception; }
		}

		private void HandleBatchException(DbException dbException) {
			exception = dbException;
			messages.AppendLine(exception.Message);
		}

		private Exception TestConnection(out DeveelDbConnection connection) {
			try {
				connection = new DeveelDbConnection(connectionString);
				connection.Open();

				if (connection.State == ConnectionState.Open)
					return null;
			} catch (Exception e) {
				connection = null;
				return e;
			}

			throw new InvalidOperationException();
		}

		private void OnQueryExecuted(SqlQuery query, int index, int queryCount) {
			if (QueryExecuted != null)
				QueryExecuted(this, new QueryEventArgs(query, index, queryCount));
		}

		public Exception TestConnection() {
			DeveelDbConnection connection;
			Exception e = TestConnection(out connection);

			if (connection != null)
				connection.Dispose();

			return e;
		}

		public void Execute(string query) {
			DeveelDbConnection connection = null;

			try {
				busy = true;

				Exception e = TestConnection(out connection);
				if (e != null)
					throw e;

				batch = new SqlQueryBatch(query);
				batch.Start();

				int queryCount = batch.QueryCount;
				for (int i = 0; i < queryCount; i++) {
					SqlQuery sqlQuery = batch[i];
					sqlQuery.Execute(connection, i);

					OnQueryExecuted(sqlQuery, i, queryCount);
				}
			} catch (DbException e) {
				HandleBatchException(e);
			} finally {
				if (batch != null)
					batch.End();

				if (connection != null)
					connection.Dispose();

				busy = false;
			}
		}
	}
}