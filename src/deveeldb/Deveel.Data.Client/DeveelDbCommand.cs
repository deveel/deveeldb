using System;
using System.Data;
using System.Data.Common;

using Deveel.Data.Protocol;

namespace Deveel.Data.Client {
	public sealed class DeveelDbCommand : DbCommand {
		private DeveelDbConnection connection;
		private bool prepared;

		private DeveelDbTransaction transaction;
		private DeveelDbParameterCollection parameters;

		private IQueryResponse[] results;

		public DeveelDbCommand() 
			: this(null) {
		}

		public DeveelDbCommand(DeveelDbConnection connection) {
			this.connection = connection;
			parameters = new DeveelDbParameterCollection(this);
		}

		public override void Prepare() {
			if (!prepared) {
				try {
					PrepareCommand();
				} finally {
					prepared = true;
				}
			}
		}

		private void PrepareCommand() {
			
		}

		public override string CommandText { get; set; }

		public override int CommandTimeout { get; set; }

		public override CommandType CommandType { get; set; }

		public override UpdateRowSource UpdatedRowSource { get; set; }

		protected override DbConnection DbConnection {
			get { return Connection; }
			set { Connection = (DeveelDbConnection) value; }
		}

		public new DeveelDbConnection Connection {
			get { return connection; }
			set { connection = value; }
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
					(value != null && value.CommitId!= transaction.CommitId))
					throw new ArgumentException("The command is already bound to another transaction.");

				transaction = value;
			}
		}

		public override bool DesignTimeVisible { get; set; }

		public override void Cancel() {
			try {
				if (results != null) {
					foreach (var result in results) {
						connection.DisposeResult(result.ResultId);
					}
				}
			} finally {
				connection.EndState();
			}
		}

		protected override DbParameter CreateDbParameter() {
			throw new NotImplementedException();
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			throw new NotImplementedException();
		}

		public override int ExecuteNonQuery() {
			throw new NotImplementedException();
		}

		public override object ExecuteScalar() {
			throw new NotImplementedException();
		}

		protected override void Dispose(bool disposing) {
			base.Dispose(disposing);
		}
	}
}
