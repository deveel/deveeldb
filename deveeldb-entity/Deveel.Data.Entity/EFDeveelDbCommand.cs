using System;
using System.Data;
using System.Data.Common;
using System.Data.Metadata.Edm;

using Deveel.Data.Client;

namespace Deveel.Data.Entity {
	internal class EFDeveelDbCommand : DbCommand, ICloneable {
		public EFDeveelDbCommand() {
			command = new DeveelDbCommand();
		}

		private DeveelDbConnection connection;
		private DeveelDbCommand command;
		private bool designTimeVisible = true;
		internal PrimitiveType[] columnTypes;

		public override void Prepare() {
			command.Prepare();
		}

		public override string CommandText {
			get { return command.CommandText; }
			set { command.CommandText = value; }
		}

		public override int CommandTimeout {
			get { return command.CommandTimeout; }
			set { command.CommandTimeout = value; }
		}

		public override CommandType CommandType {
			get { return command.CommandType; }
			set { command.CommandType = value; }
		}

		public override UpdateRowSource UpdatedRowSource {
			get { return command.UpdatedRowSource; }
			set { command.UpdatedRowSource = value; }
		}

		protected override DbConnection DbConnection {
			get { return connection; }
			set {
				connection = (DeveelDbConnection) value;
				command.Connection = connection;
			}
		}

		protected override DbParameterCollection DbParameterCollection {
			get { return command.Parameters; }
		}

		protected override DbTransaction DbTransaction {
			get { return command.Transaction; }
			set { command.Transaction = (DeveelDbTransaction) value; }
		}

		public override bool DesignTimeVisible {
			get { return designTimeVisible; }
			set { designTimeVisible = value; }
		}

		public override void Cancel() {
			command.Cancel();
		}

		protected override DbParameter CreateDbParameter() {
			return new DeveelDbParameter();
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			throw new NotImplementedException();
		}

		public override int ExecuteNonQuery() {
			return command.ExecuteNonQuery();
		}

		public override object ExecuteScalar() {
			return command.ExecuteScalar();
		}

		public object Clone() {
			EFDeveelDbCommand cmd = new EFDeveelDbCommand();
			cmd.command = (DeveelDbCommand)command.Clone();
			cmd.columnTypes = (PrimitiveType[]) columnTypes.Clone();
			return cmd;
		}
	}
}