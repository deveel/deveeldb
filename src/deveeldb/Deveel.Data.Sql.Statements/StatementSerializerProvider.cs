using System;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	class StatementSerializerProvider : ObjectSerializerProvider {
		protected override void Init() {
			Register<AlterTableStatement.Prepared, AlterTableStatement.PreparedSerializer>();
		}
	}
}
