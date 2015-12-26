using System;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropCallbackTriggersStatement : SqlStatement {
		public DropCallbackTriggersStatement(ObjectName tableName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;
		}

		private DropCallbackTriggersStatement(ObjectData data)
			: base(data) {
			TableName = data.GetValue<ObjectName>("TableName");
		}

		public ObjectName TableName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			base.ExecuteStatement(context);
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("TableName", TableName);
		}
	}
}