using System;
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class RenameTriggerAction : IAlterTriggerAction, ISerializable, IStatementPreparable {
		public RenameTriggerAction(ObjectName name) {
			if (name == null)
				throw new ArgumentNullException("name");

			Name = name;
		}

		private RenameTriggerAction(SerializationInfo info, StreamingContext context) {
			Name = (ObjectName) info.GetValue("Name", typeof(ObjectName));
		}

		public ObjectName Name { get; private set; }

		AlterTriggerActionType IAlterTriggerAction.ActionType {
			get { return AlterTriggerActionType.Rename; }
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			builder.Append("RENAME TO ");
			Name.AppendTo(builder);
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Name", Name);
		}

		object IStatementPreparable.Prepare(IRequest request) {
			var name = request.Access().ResolveObjectName(DbObjectType.Trigger, Name);
			return new RenameTriggerAction(name);
		}
	}
}
