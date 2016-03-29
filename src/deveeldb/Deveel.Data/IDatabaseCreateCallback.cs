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

namespace Deveel.Data {
	/// <summary>
	/// Defines the callback that a <see cref="IDatabase.Create"/> function
	/// calls right before the finalization of the database initial state.
	/// </summary>
	/// <remarks>
	/// <para>
	/// External features will be able to implement a callback to create
	/// special objects after the core is generated, still in scope of the
	/// creation process.
	/// </para>
	/// <para>
	/// To activate this function the external features will also be
	/// required to register this to <see cref="ISystemContext.ServiceProvider"/>.
	/// </para>
	/// </remarks>
	public interface IDatabaseCreateCallback {
		/// <summary>
		/// Called when the database is created and before the
		/// finalization of the initialization process.
		/// </summary>
		/// <param name="context">The privileged system context that
		/// is used to generate the initial database.</param>
		void OnDatabaseCreate(IQuery context);
	}
}
