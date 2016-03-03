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

namespace Deveel.Data.Security {
	[Flags]
	public enum Privileges {
		None = 0,
		Create = 1,
		Alter = 2,
		Delete = 64,
		Drop = 4,
		Insert = 128,
		List = 8,
		References = 256,
		Select = 16,
		Update = 32,
		Usage = 512,
		Compact = 1024,
		Execute = 2048,

		All = Alter | Compact | Create | Delete |
		      Drop | Insert | List | References |
		      Select | Update | Usage,

		TableAll = Select | Update | Delete | Insert | References | Usage | Compact,
		TableRead = Select | Usage,

		SchemaAll = Create | Alter | Drop | List,
		SchemaRead = List,
	}
}