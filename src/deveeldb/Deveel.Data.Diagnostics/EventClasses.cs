// 
//  Copyright 2010-2014 Deveel
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

using System;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// Provides a finite set of classes for events.
	/// </summary>
	public static class EventClasses {
		public const int System = 0x000120;
		public const int Compiler = 0x003033;
		public const int Storage = 0x000670;
		public const int Runtime = 0x008880;
		public const int Expressions = 0x0050030;
	}
}