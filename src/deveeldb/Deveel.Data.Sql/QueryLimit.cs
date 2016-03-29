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

namespace Deveel.Data.Sql {
	public sealed class QueryLimit {
		public QueryLimit(long total) 
			: this(-1, total) {
		}

		public QueryLimit(long offset, long count) {
			if (count < 1)
				throw new ArgumentException("The limit clause must have at least one element.");
			if (offset < 0)
				offset = 0;

			Offset = offset;
			Count = count;
		}

		public long Offset { get; private set; }

		public long Count { get; private set; }
	}
}
