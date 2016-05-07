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
	/// Defines a callback from <see cref="Database"/> during the
	/// creation phase.
	/// </summary>
	/// <remarks>
	/// <para>
	/// <see cref="Database"/> class calls instances of this interface,
	/// registered in the system context, at the moment of creation,
	/// passing an instance of <see cref="IQuery"/> that has system
	/// authorization and that can be used to operate on the database
	/// initial structure.
	/// </para>
	/// <para>
	/// When <see cref="OnDatabaseCreate"/> method is called, the database
	/// instance that is calling it is in the initial state, that means
	/// it does contain only the system tables necessary to operate.
	/// </para>
	/// <para>
	/// This callback is ideal for those components that must register
	/// tables and other sub-components
	/// </para>
	/// <para>
	/// All the <see cref="IDatabaseCreateCallback"/> instances are resolved
	/// and invoked before <see cref="IDatabaseCreatedCallback"/> instances,
	/// in the database creation life-cycle.
	/// </para>
	/// </remarks>
	/// <seealso cref="Database.Create(string,string)"/>
	/// <seealso cref="Database"/>
	/// <seealso cref="IDatabaseCreatedCallback"/>
	public interface IDatabaseCreateCallback {
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
		void OnDatabaseCreate(IQuery systemQuery);
	}
}
