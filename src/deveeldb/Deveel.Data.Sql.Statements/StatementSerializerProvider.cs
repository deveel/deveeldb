using System;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	class StatementSerializerProvider : ObjectSerializerProvider {
		protected override void Init() {
			Register<AlterTableStatement.Prepared, AlterTableStatement.PreparedSerializer>();
			Register<AlterUserStatement.Prepared, AlterUserStatement.PreparedSerializer>();
			Register<LoopControlStatement.Prepared, LoopControlStatement.Serializer>();
			Register<CloseStatement, CloseStatement.Serializer>();
		}
	}
}
