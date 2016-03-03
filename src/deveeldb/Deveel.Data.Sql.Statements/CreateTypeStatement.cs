using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateTypeStatement : SqlStatement, IPreparableStatement {
		public CreateTypeStatement(ObjectName typeName, IEnumerable<UserTypeMember> members) {
			if (typeName == null)
				throw new ArgumentNullException("typeName");
			if (members == null)
				throw new ArgumentNullException("members");

			if (!members.Any())
				throw new ArgumentException("At least one member must be specified.");

			TypeName = typeName;
			Members = members;
		}

		public ObjectName TypeName { get; private set; }

		public bool ReplaceIfExists { get; set; }

		public IEnumerable<UserTypeMember> Members { get; private set; }

		IStatement IPreparableStatement.Prepare(IRequest request) {
			// TODO: resolve the type name
			return new Prepared(TypeName, Members.ToArray(), ReplaceIfExists);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(ObjectName typeName, UserTypeMember[] members, bool replaceIfExists) {
				TypeName = typeName;
				Members = members;
				ReplaceIfExists = replaceIfExists;
			}

			private Prepared(ObjectData data) {
				TypeName = data.GetValue<ObjectName>("TypeName");
				Members = data.GetValue<UserTypeMember[]>("Members");
				ReplaceIfExists = data.GetBoolean("Replace");
			}

			public ObjectName TypeName { get; private set; }

			public UserTypeMember[] Members { get; private set; }

			public bool ReplaceIfExists { get; set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("TypeName", TypeName);
				data.SetValue("Members", Members);
				data.SetValue("Replace", ReplaceIfExists);
			}
		}

		#endregion
	}
}