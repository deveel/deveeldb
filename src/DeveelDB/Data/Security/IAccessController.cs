// 
//  Copyright 2010-2018 Deveel
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
using System.Threading.Tasks;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	/// <summary>
	/// The system that verifies that an user or a role owns
	/// privileges over a database objects 
	/// </summary>
	public interface IAccessController {
		/// <summary>
		/// Checks if a given grantee owns a specific privilege over
		/// the indicated object
		/// </summary>
		/// <param name="grantee">The grantee to verify</param>
		/// <param name="objType">The type of database object</param>
		/// <param name="objName">The name of the database object to verify</param>
		/// <param name="privilege">The privilege that needs to be verified</param>
		/// <returns>
		/// Returns a boolean value indicating if the given grantee owns
		/// the specified privilege over the indicated object
		/// </returns>
		Task<bool> HasPrivilegesAsync(string grantee, DbObjectType objType, ObjectName objName, Privilege privilege);
	}
}