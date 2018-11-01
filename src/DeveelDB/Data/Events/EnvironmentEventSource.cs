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
using System.Collections.Generic;

namespace Deveel.Data.Events {
	class EnvironmentEventSource : EventSource {
		protected override void GetMetadata(Dictionary<string, object> metadata) {
			metadata["env.machine"] = System.Environment.MachineName;
			metadata["env.os"] = System.Environment.OSVersion;

			var variables = System.Environment.GetEnvironmentVariables();
			foreach (var variable in variables.Keys) {
				metadata[$"env.{variable}"] = variables[variable];
			}
		}
	}
}