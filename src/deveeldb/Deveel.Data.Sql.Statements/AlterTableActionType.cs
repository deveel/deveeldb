using System;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// The possible types of actions in a <see cref="AlterTableAction"/>
	/// expression.
	/// </summary>
	public enum AlterTableActionType {
		/// <summary>
		/// Adds a defined column to a table.
		/// </summary>
		AddColumn = 1,

		/// <summary>
		/// Modifies a table by removing a given column.
		/// </summary>
		DropColumn = 2,

		/// <summary>
		/// Adds a new constraint to the table.
		/// </summary>
		AddConstraint = 3,

		/// <summary>
		/// Drops a named constraint from a table.
		/// </summary>
		DropConstraint = 4,

		/// <summary>
		/// Drops a <c>PRIMARY KEY</c> constraint from a table.
		/// </summary>
		DropPrimaryKey = 5,

		/// <summary>
		/// Alters a table column setting the <c>DEFAULT</c> expression.
		/// </summary>
		SetDefault = 6,

		/// <summary>
		/// Drops the <c>DEFAULT</c> expression from a given column.
		/// </summary>
		DropDefault = 7,
	}
}
