using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Statements;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Parser {
	class CreateTypeNode : SqlStatementNode {
		public string TypeName { get; private set; }

		public bool ReplaceIfExists { get; private set; }

		public IEnumerable<TypeAttributeNode> Attributes { get; private set; } 

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var typeName = ObjectName.Parse(TypeName);
			var members = Attributes.Select(x => {
				var type = DataTypeBuilder.Build(builder.TypeResolver, x.Type);
				return new UserTypeMember(x.Name, type);
			});

			builder.AddObject(new CreateTypeStatement(typeName, members) {
				ReplaceIfExists = ReplaceIfExists
			});
		}
	}
}