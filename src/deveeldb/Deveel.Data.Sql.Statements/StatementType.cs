using System;

namespace Deveel.Data.Sql.Statements {
	public enum StatementType {
		// Schema
		CreateSchema,
		DropSchema,

		// Tables
		CreateTable,
		AlterTable,
		DropTable,

		CreateView,
		DropView,

		// Security
		CreateGroup,
		DropGroup,
		CreateUser,
		DropUser,
		Grant,
		Revoke,

		// CRUD
		Select,
		Insert,
		Update
	}
}
