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

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// An event fired when a database object of the given type is created 
	/// during the lifetime of a transaction. 
	/// </summary>
	/// <remarks>
	/// The object created is identified in a transaction by the given
	/// <see cref="DbObjectType"/> and its unique <see cref="ObjectName"/>.
	/// </remarks>
	/// <seealso cref="DbObjectType"/>
	/// <seealso cref="ObjectName"/>
	public class ObjectCreatedEvent : ITransactionEvent {
		/// <summary>
		/// Constructs a new event with the given object name and type.
		/// </summary>
		/// <param name="objectName">The <see cref="ObjectName"/> that uniquely identify the
		/// object created.</param>
		/// <param name="objectType">The <see cref="DbObjectType"/> of the object created.</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="objectName"/> parameter passed is <c>null</c>.
		/// </exception>
		public ObjectCreatedEvent(ObjectName objectName, DbObjectType objectType) {
			if (objectName == null)
				throw new ArgumentNullException("objectName");

			ObjectName = objectName;
			ObjectType = objectType;
		}

		/// <summary>
		/// Gets the <see cref="ObjectName"/> that uniquely identify the
		/// object created.
		/// </summary>
		public ObjectName ObjectName { get; private set; }

		/// <summary>
		/// Gets the type of the object created.
		/// </summary>
		public DbObjectType ObjectType { get; private set; }
	}
}
