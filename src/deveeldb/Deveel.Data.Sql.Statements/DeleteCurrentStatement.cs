using System;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	public sealed class DeleteCurrentStatement : SqlStatement {
		public DeleteCurrentStatement(ObjectName tableName, string cursorName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			TableName = tableName;
			CursorName = cursorName;
		}

		public ObjectName TableName { get; private set; }

		public string CursorName { get; private set; }

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(ObjectName tableName, string cursorName) {
				TableName = tableName;
				CursorName = cursorName;
			}

			private Prepared(ObjectData data) {
				TableName = data.GetValue<ObjectName>("TableName");
				CursorName = data.GetString("CursorName");
			}

			public ObjectName TableName { get; private set; }

			public string CursorName { get; private set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("TableName", TableName);
				data.SetValue("CursorName", CursorName);
			}
		}

		#endregion
	}
}