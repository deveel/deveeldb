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
