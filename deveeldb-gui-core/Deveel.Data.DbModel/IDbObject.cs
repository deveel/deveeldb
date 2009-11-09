using System;

namespace Deveel.Data.DbModel {
	/// <summary>
	/// This interface represents a generic object on a database.
	/// </summary>
	public interface IDbObject {
		/// <summary>
		/// Gets the name of the schema that contains the object.
		/// </summary>
		string Schema { get; }

		/// <summary>
		/// Gets the name of the object.
		/// </summary>
		string Name { get; }

		/// <summary>
		/// Gets the specific type of the object.
		/// </summary>
		DbObjectType ObjectType { get; }
	}
}