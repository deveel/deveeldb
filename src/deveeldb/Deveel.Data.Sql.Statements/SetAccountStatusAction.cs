using System;
using System.IO;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class SetAccountStatusAction : IAlterUserAction {
		public SetAccountStatusAction(UserStatus status) {
			Status = status;
		}

		public UserStatus Status { get; private set; }

		public AlterUserActionType ActionType {
			get { return AlterUserActionType.SetAccountStatus; }
		}

		public static void Serialize(SetAccountStatusAction action, BinaryWriter writer) {
			writer.Write((byte)action.Status);
		}

		public static SetAccountStatusAction Deserialize(BinaryReader reader) {
			var status = (UserStatus) reader.ReadByte();
			return new SetAccountStatusAction(status);
		} 
	}
}
