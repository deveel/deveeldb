// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;
using System.Runtime.Serialization;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SetAccountStatusAction : IAlterUserAction {
		public SetAccountStatusAction(UserStatus status) {
			Status = status;
		}

		private SetAccountStatusAction(SerializationInfo info, StreamingContext context) {
			Status = (UserStatus) info.GetInt32("Status");
		}

		public UserStatus Status { get; private set; }

		public AlterUserActionType ActionType {
			get { return AlterUserActionType.SetAccountStatus; }
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Status", (int) Status);
		}

		//public static void Serialize(SetAccountStatusAction action, BinaryWriter writer) {
		//	writer.Write((byte)action.Status);
		//}

		//public static SetAccountStatusAction Deserialize(BinaryReader reader) {
		//	var status = (UserStatus) reader.ReadByte();
		//	return new SetAccountStatusAction(status);
		//} 
	}
}
