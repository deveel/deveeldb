﻿// 
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
using System.Collections.Generic;
using System.Threading.Tasks;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public interface IGrantManager : IAccessController {
		Task<bool> RevokeFromUserAsync(string revoker, string user, ObjectName objName, Privilege privileges, bool option);

		Task<IEnumerable<Grant>> GetGrantsAsync(string grantee);

		Task<IEnumerable<Grant>> GetGrantedAsync(string granter);

		Task<bool> GrantToUserAsync(string granter, string user, ObjectName objName, Privilege privileges, bool withOption);

		Task<bool> GrantToRoleAsync(string role, ObjectName objName, Privilege privilege);

		Task<bool> RevokeFromRoleAsync(string role, ObjectName objName, Privilege privilege);
	}
}