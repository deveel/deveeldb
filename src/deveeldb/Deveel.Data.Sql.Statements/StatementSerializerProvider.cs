using System;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	class StatementSerializerProvider : ObjectSerializerProvider {
		protected override void Init() {
			Register<AlterTableStatement.Prepared, AlterTableStatement.PreparedSerializer>();
			Register<AlterUserStatement.Prepared, AlterUserStatement.PreparedSerializer>();
			Register<LoopControlStatement.Prepared, LoopControlStatement.Serializer>();
			Register<CloseStatement, CloseStatement.Serializer>();
			Register<CreateTableStatement.Prepared, CreateTableStatement.PreparedSerializer>();
			Register<CreateUserStatement.Prepared, CreateUserStatement.PreparedSerializer>();
			Register<CreateViewStatement.Prepared, CreateViewStatement.PreparedSerializer>();
			Register<DeclareCursorStatement, DeclareCursorStatement.Serializer>();
			Register<DropTableStatement.Prepared, DropTableStatement.PreparedSerializer>();
			Register<DropViewStatement.Prepared, DropViewStatement.PreparedSerializer>();
			Register<InsertSelectStatement.Prepared, InsertSelectStatement.PreparedSerializer>();
			Register<InsertStatement.Prepared, InsertStatement.PreparedSerializer>();
			Register<OpenStatement.Prepared, OpenStatement.PreparedSerializer>();
			Register<SelectIntoStatement.Prepared, SelectIntoStatement.PreparedSerializer>();
		}
	}
}
