using System;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropTypeStatement : SqlStatement {
		public DropTypeStatement(ObjectName typeName) {
			if (typeName == null)
				throw new ArgumentNullException("typeName");

			TypeName = typeName;
		}

		private DropTypeStatement(ObjectData data) {
			TypeName = data.GetValue<ObjectName>("TypeName");
		}

		public ObjectName TypeName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			base.ExecuteStatement(context);
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("TypeName", TypeName);
		}
	}
}