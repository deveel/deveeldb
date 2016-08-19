using System;
using System.Runtime.Serialization;
using System.Threading;

using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class LockTableStatement : SqlStatement {
		public LockTableStatement(ObjectName tableName, LockingMode mode) 
			: this(tableName, mode, Timeout.Infinite) {
		}

		public LockTableStatement(ObjectName tableName, LockingMode mode, int timeout) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (timeout < Timeout.Infinite)
				throw new ArgumentException("Invalid wait timeout specified", "timeout");

			TableName = tableName;
			Mode = mode;
			WaitTimeout = timeout;
		}

		private LockTableStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			TableName = (ObjectName) info.GetValue("TableName", typeof(ObjectName));
			Mode = (LockingMode) info.GetInt32("Mode");
			WaitTimeout = info.GetInt32("Wait");
		}

		public ObjectName TableName { get; private set; }

		public int WaitTimeout { get; set; }

		public LockingMode Mode { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TableName", TableName);
			info.AddValue("Mode", (int) Mode);
			info.AddValue("Wait", WaitTimeout);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("LOCK TABLE ");
			TableName.AppendTo(builder);
			builder.AppendFormat(" IN {0} MODE", Mode.ToString().ToUpperInvariant());

			if (WaitTimeout >= 0) {
				if (WaitTimeout == 0) {
					builder.Append(" NOWAIT");
				} else {
					builder.AppendFormat(" WAIT {0}", WaitTimeout);
				}
			}
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableName = context.Access().ResolveObjectName(TableName);
			return new LockTableStatement(tableName, Mode, WaitTimeout);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.ObjectExists(TableName))
				throw new ObjectNotFoundException(TableName);

			context.DirectAccess.LockTable(TableName, Mode, WaitTimeout);
		}
	}
}
