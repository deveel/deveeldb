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
