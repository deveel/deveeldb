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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Defines the contract for the business managers of database objects of a given type.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Implementations of this interface will allow the system to:
	/// <list type="bullet">
	/// <item>Create the required tables into the system at initialization</item>
	/// <item>Create database objects given a <see cref="IObjectInfo"/> specification</item>
	/// <item>Alter an existing object</item>
	/// <item>Assess the existence of a given object by name</item>
	/// <item>Get the instance of an object by its unique name</item>
	/// <item>Delete an object from the system</item>
	/// <item>Normalize the name of an object</item>
	/// </list>
	/// </para>
	/// </remarks>
	public interface IObjectManager : IDisposable {
		/// <summary>
		/// Gets the type of objects managed by this instance.
		/// </summary>
		/// <seealso cref="DbObjectType"/>
		DbObjectType ObjectType { get; }

		/// <summary>
		/// Create a new object of the <see cref="ObjectType"/> given the
		/// specifications given.
		/// </summary>
		/// <param name="objInfo">The object specifications used to create a new object.</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="objInfo"/> is <c>null</c>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If the object type of the specification (<see cref="IObjectInfo.ObjectType"/>) is
		/// different than the <see cref="ObjectType"/> of this manager.
		/// </exception>
		void CreateObject(IObjectInfo objInfo);

		/// <summary>
		/// Checks if an object really exists in the system.
		/// </summary>
		/// <param name="objName">The unique name of the object to check.</param>
		/// <returns>
		/// Returns <c>true</c> if an object with the given name concretely exists in the
		/// system, or <c>false</c> otherwise.
		/// </returns>
		bool RealObjectExists(ObjectName objName);

		/// <summary>
		/// Checks if an object identified by the given name is managed by this instance. 
		/// </summary>
		/// <param name="objName">The name that uniquely identifies the object.</param>
		/// <returns>
		/// </returns>
		bool ObjectExists(ObjectName objName);

		/// <summary>
		/// Gets a database object managed by this manager.
		/// </summary>
		/// <param name="objName">The name that uniquely identifies the object to get.</param>
		/// <returns>
		/// Returns a <see cref="IDbObject"/> instance that is identified by the given unique name,
		/// or <c>null</c> if this manager was not able to map any object to the name specified.
		/// </returns>
		IDbObject GetObject(ObjectName objName);

		/// <summary>
		/// Modifies an existing object managed, identified by <see cref="IObjectInfo.FullName"/> component
		/// of the given specification, with the format given.
		/// </summary>
		/// <param name="objInfo">The object specification used to alter an existing object.</param>
		/// <returns>
		/// Returns <c>true</c> an object was identified and successfully altered, or <c>false</c> if none
		/// database object was found for the unique name given.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="objInfo"/> object is <c>null</c>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If the type of the object specified (<see cref="IObjectInfo.ObjectType"/>) is different than the
		/// type of objects handled by this manager.
		/// </exception>
		bool AlterObject(IObjectInfo objInfo);

		/// <summary>
		/// Deletes a database object handled by this manager from the system.
		/// </summary>
		/// <param name="objName">The unique name of the object to be deleted.</param>
		/// <returns>
		/// Returns <c>true</c> if a database object was found with the given unique name and successfully 
		/// deleted from the system, or <c>false</c> if none object was found.
		/// </returns>
		bool DropObject(ObjectName objName);

		/// <summary>
		/// Normalizes the input object name using the case sensitivity specified.
		/// </summary>
		/// <param name="objName">The input object name, that can be partial or complete, to
		/// be normalized to the real name of an object.</param>
		/// <param name="ignoreCase">The case sensitivity specification used to compare the
		/// input name with the names of the existing objects handled by this manager.</param>
		/// <returns>
		/// Returns the fully normalized <see cref="ObjectName"/> that is the real name of an object
		/// matching the input name, or <c>null</c> if the input name was not possible to be
		/// resolved.
		/// </returns>
		ObjectName ResolveName(ObjectName objName, bool ignoreCase);
	}
}
