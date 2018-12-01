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

using Deveel.Data.Configurations;

namespace Deveel.Data.Security {
	/// <summary>
	/// Provides mechanisms for the authentication of users
	/// to the system, given a set of information
	/// </summary>
	public interface IAuthenticator {
		/// <summary>
		/// Authenticates a user identified by the given name and the
		/// information provided as argument 
		/// </summary>
		/// <param name="userName">The name of the user to authenticate</param>
		/// <param name="identification">The information for the identification of
		/// the user</param>
		/// <returns>
		/// Returns an instance of <see cref="User"/> identified by the given name and
		/// the information provided as argument
		/// </returns>
		/// <exception cref="NotSupportedException">
		/// Thrown if the provided <see cref="IUserIdentification"/> argument is not
		/// supported by this instance of the authenticator service
		/// </exception>
		/// <exception cref="AuthenticationException">
		/// Thrown when an error occurs during the authentication process
		/// </exception>
		Task<User> AuthenticateAsync(string userName, IUserIdentification identification);
	}
}