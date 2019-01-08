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

using Deveel.Data.Security;
using Deveel.Data.Services;

using Moq;

namespace Deveel.Data.Sql.Statements {
	static class MockedSession {
		public static ISession Create(IContext parent, User user) {
			var userSession = new Mock<ISession>();
			userSession.SetupGet(x => x.User)
				.Returns(user);
			userSession.SetupGet(x => x.Scope)
				.Returns(parent.Scope.OpenScope(KnownScopes.Session));
			userSession.SetupGet(x => x.ParentContext)
				.Returns(parent);

			return userSession.Object;
		}
	}
}