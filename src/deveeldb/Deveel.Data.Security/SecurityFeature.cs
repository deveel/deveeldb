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

using Deveel.Data.Build;
using Deveel.Data.Services;
using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class SecurityFeature : ISystemFeature {
		public string Name {
			get { return "Security"; }
		}

		public string Version {
			get { return "2.0"; }
		}

		public void OnBuild(ISystemBuilder builder) {
			builder
				.Use<IUserManager>(options => options
					.With<UserManager>()
					.InSessionScope())
				//.Use<IDatabaseCreateCallback>(options => options
				//	.With<UsersInit>()
				//	.InQueryScope())
				.Use<IPrivilegeManager>(options => options
					.With<PrivilegeManager>()
					.InSessionScope())
				.Use<ITableCompositeSetupCallback>(options => options
					.With<PrivilegesInit>()
					.InQueryScope())
				.Use<IUserIdentifier>(options => options
					.With<ClearTextUserIdentifier>());

			// TODO: Add the system callbacks

#if !PCL
			builder.Use<IUserIdentifier, Pkcs12UserIdentifier>();
#endif
		}

		public void OnSystemEvent(SystemEvent @event) {
			throw new NotImplementedException();
		}
	}
}
