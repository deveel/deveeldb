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

using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// A callback from <see cref="Database"/> after the initial
	/// creation of the database.
	/// </summary>
	/// <remarks>
	/// <para>
	/// <see cref="Database"/> class calls instances of this interface,
	/// registered in the system context, after the creation of the database,
	/// passing an instance of <see cref="IQuery"/> that has system
	/// authorization and that can be used to operate on the database.
	/// </para>
	/// <para>
	/// When <see cref="OnDatabaseCreated"/> method is called, the database
	/// instance that is calling it is in the state after all the <see cref="IDatabaseCreateCallback"/>, 
	/// that means it does contain the system tables necessary to operate and
	/// all the tables and sub-components registered from the previous callbacks.
	/// </para>
	/// <para>
	/// All the <see cref="IDatabaseCreatedCallback"/> instances are resolved
	/// and invoked after <see cref="IDatabaseCreateCallback"/> instances,
	/// in the database creation life-cycle.
	/// </para>
	/// </remarks>
	/// <seealso cref="Database.Create"/>
	/// <seealso cref="Database"/>
	/// <seealso cref="IDatabaseCreateCallback"/>
	public interface IDatabaseCreatedCallback {
		/// <summary>
		/// Operates the routines of the callback over
		/// the passed <see cref="IQuery"/> instance, that has
		/// administrative rights over the underlying database. 
		/// </summary>
		/// <param name="systemQuery">The <see cref="IQuery"/> object that
		/// the callback can use to interact with the database.</param>
		/// <remarks>
		/// <para>
		/// Any error thrown during the execution of this method will void
		/// any operation done in the context. The error will arise an
		/// <see cref="ErrorEvent"/> over the existing context of the database,
		/// but it will not be fired in foreground.
		/// </para>
		/// </remarks>
		void OnDatabaseCreated(IQuery systemQuery);
	}
}
