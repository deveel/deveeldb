// 
//  Copyright 2010-2015 Deveel
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

namespace Deveel.Data.Routines {
	/// <summary>
	/// The contract to define a program routine that can
	/// interact with database objects.
	/// </summary>
	public interface IRoutine {
        /// <summary>
        /// Gets the type of routine that will be executed.
        /// </summary>
        /// <seealso cref="RoutineType"/>
		RoutineType Type { get; }

        /// <summary>
        /// Gets the full name of the routine within the system.
        /// </summary>
        /// <seealso cref="ObjectName"/>
		ObjectName Name { get; }

        /// <summary>
        /// Gets an optional list of parameters that the routine
        /// can handle.
        /// </summary>
        /// <seealso cref="RoutineParameter"/>
		RoutineParameter[] Parameters { get; }

        /// <summary>
        /// Executes the routine within the context given and
        /// returning a result.
        /// </summary>
        /// <param name="context">The context in which the routine
        /// is executed.</param>
        /// <returns>
        /// Returns an instance of <see cref="ExecuteResult"/> that contains
        /// the result of the exection of the routine.
        /// </returns>
		ExecuteResult Execute(ExecuteContext context);
	}
}