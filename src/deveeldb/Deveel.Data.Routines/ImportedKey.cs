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
	internal static class ImportedKey {
		public const int Cascade = 0;
		public const int InitiallyDeferred = 5;
		public const int InitiallyImmediate = 6;
		public const int NoAction = 3;
		public const int NotDeferrable = 7;
		public const int Restrict = 1;
		public const int SetDefault = 4;
		public const int SetNull = 2;
	}
}